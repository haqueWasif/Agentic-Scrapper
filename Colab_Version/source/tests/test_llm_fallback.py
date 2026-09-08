"""Offline OpenRouter-only provider and cross-thread UI notification checks."""

import ast
import asyncio
import json
import os
import re
import threading
import unittest
from contextvars import ContextVar
from pathlib import Path
from queue import Empty, Queue
from types import SimpleNamespace
from typing import Any, Callable, TypedDict
from unittest.mock import patch


def load_evaluator(outcomes):
    path = Path(__file__).resolve().parents[1] / 'app' / 'agents' / 'orchestrator.py'
    tree = ast.parse(path.read_text(encoding='utf-8'))
    nodes = [node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
             or (isinstance(node, ast.Assign) and any(isinstance(target, ast.Name)
                 and target.id == 'JSON_ONLY_INSTRUCTION' for target in node.targets))]
    models, crews = [], []

    def llm(**kwargs):
        result = SimpleNamespace(**kwargs)
        models.append(result)
        return result

    class Crew:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)
            self.calls = 0
            self.models_at_kickoff = []
            crews.append(self)

        def kickoff(self):
            self.models_at_kickoff.append([agent.llm for agent in self.agents])
            outcome = outcomes[self.calls]
            self.calls += 1
            if isinstance(outcome, Exception):
                raise outcome
            return SimpleNamespace(raw=outcome)

    namespace = dict(
        asyncio=asyncio, os=os, json=json, re=re, Any=Any, Callable=Callable, TypedDict=TypedDict,
        Queue=Queue, Empty=Empty,
        _status_messages=ContextVar('test-status', default=None),
        LLM=llm, Agent=lambda **kw: SimpleNamespace(**kw),
        Task=lambda **kw: SimpleNamespace(**kw), Crew=Crew,
        StatelessCrew=Crew, prepare_storage=lambda: 'runtime/crewai',
    )
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(path), 'exec'), namespace)
    return namespace, models, crews


STATE = {'raw_html': '', 'markdown_content': 'HVAC book table', 'extracted_documents': []}


class OpenRouterTests(unittest.TestCase):
    def setUp(self):
        env = patch.dict(os.environ, {'OPENROUTER_API_KEY': 'router-test'})
        env.start()
        self.addCleanup(env.stop)

    def test_first_request_uses_openrouter_everywhere(self):
        ns, models, crews = load_evaluator(['[]'])
        self.assertEqual(ns['evaluate_documents'](STATE), {'extracted_documents': []})
        self.assertEqual(len(models), 1)
        primary = models[0]
        self.assertEqual(primary.model, 'openrouter/openrouter/free')
        self.assertEqual(primary.api_key, 'router-test')
        self.assertFalse(hasattr(primary, 'base_url'))
        self.assertEqual(crews[0].calls, 1)
        self.assertEqual(crews[0].models_at_kickoff, [[primary]])
        for field in ('manager_llm', 'function_calling_llm', 'planning_llm', 'chat_llm'):
            self.assertIs(getattr(crews[0], field), primary)
        for agent in crews[0].agents:
            self.assertIs(agent.llm, primary)
            self.assertIs(agent.function_calling_llm, primary)
        for task in crews[0].tasks:
            self.assertIs(task.agent.llm, primary)
            self.assertFalse(hasattr(task, 'output_json'))
            self.assertFalse(hasattr(task, 'output_pydantic'))

    def test_provider_errors_do_not_switch_or_retry(self):
        for message in ('429 quota exceeded', 'RESOURCE_EXHAUSTED'):
            with self.subTest(message=message):
                ns, models, crews = load_evaluator([RuntimeError(message)])
                with self.assertRaisesRegex(RuntimeError, message):
                    ns['evaluate_documents'](STATE)
                self.assertEqual(crews[0].calls, 1)
                self.assertEqual(len(models), 1)
                self.assertEqual(models[0].model, 'openrouter/openrouter/free')

    def test_nonquota_error_does_not_switch(self):
        ns, models, crews = load_evaluator([RuntimeError('401 unauthorized')])
        with self.assertRaisesRegex(RuntimeError, '401'):
            ns['evaluate_documents'](STATE)
        self.assertEqual(len(models), 1)
        self.assertEqual(crews[0].calls, 1)

    def test_missing_key_reports_configuration_error(self):
        for key in ('', '   '):
            ns, models, crews = load_evaluator([])
            with patch.dict(os.environ, {'OPENROUTER_API_KEY': key}):
                with self.assertRaisesRegex(RuntimeError, 'OPENROUTER_API_KEY'):
                    ns['evaluate_documents'](STATE)
            self.assertFalse(crews)
            self.assertFalse(models)

    def test_preserves_structured_document_output(self):
        ns, _, _ = load_evaluator(['```json\n[{"title":"HVAC","link":"/book/123"}]\n```'])
        self.assertEqual(ns['evaluate_documents'](STATE), {
            'extracted_documents': [{'title': 'HVAC', 'link': '/book/123'}],
        })

    def test_empty_markdown_does_not_create_llm(self):
        ns, models, crews = load_evaluator([])
        with self.assertRaisesRegex(ValueError, 'markdown_content'):
            ns['evaluate_documents']({**STATE, 'markdown_content': ''})
        self.assertFalse(models or crews)

    def test_invalid_json_returns_empty_results_for_fallback(self):
        ns, _, crews = load_evaluator(['not JSON'])
        self.assertEqual(ns['evaluate_documents'](STATE), {'extracted_documents': []})
        self.assertEqual(crews[0].calls, 1)

    def test_json_only_instruction_in_agent_and_task_prompts(self):
        ns, _, crews = load_evaluator(['[]'])
        ns['evaluate_documents'](STATE)
        instruction = ns['JSON_ONLY_INSTRUCTION']
        agent, task = crews[0].agents[0], crews[0].tasks[0]
        for prompt in (agent.goal, agent.backstory, task.description, task.expected_output):
            self.assertIn(instruction, prompt)
        self.assertTrue(task.description.endswith(instruction))

    def test_conversational_fenced_output_is_cleaned_in_evaluation(self):
        ns, _, _ = load_evaluator(['Here are the approved documents:\n```json\n[{"title":"HVAC"}]\n```\nDone.'])
        self.assertEqual(ns['evaluate_documents'](STATE), {'extracted_documents': [{'title': 'HVAC'}]})


class JsonCleaningTests(unittest.TestCase):
    def setUp(self):
        self.ns, _, _ = load_evaluator([])

    def test_supported_fence_styles_and_empty_array(self):
        for text in ('[]', '  []  ', '```json\n[]\n```', '```JSON\r\n[]\r\n```',
                     '```\n[]\n```', '```[]```', '```json []```',
                     'Results:\n```json\n[]\n```\nNo documents match.'):
            with self.subTest(text=text):
                self.assertEqual(self.ns['_parse_document_list'](SimpleNamespace(raw=text)), [])

    def test_raw_json_preserves_literal_backticks(self):
        documents = [{'title': 'HVAC', 'relevance_reason': 'Literal ```json and ``` markers'}]
        self.assertEqual(self.ns['_parse_document_list'](json.dumps(documents)), documents)

    def test_explicit_json_fence_takes_priority_over_other_blocks(self):
        text = '```text\nintro\n```\n```json\n[{"title":"HVAC"}]\n```'
        self.assertEqual(self.ns['_parse_document_list'](text), [{'title': 'HVAC'}])

    def test_malformed_or_truncated_payloads_request_fallback(self):
        for text in ('', 'Here are the results: []', '```json\n[{"title":}]\n```',
                     '```json\n[]', '[{"title":"HVAC"}'):
            with self.subTest(text=text):
                self.assertEqual(self.ns['_parse_document_list'](text), [])

    def test_non_array_or_non_object_items_still_rejected(self):
        for text in ('{}', 'null', '"hello"', '[1]', '```json\n["HVAC"]\n```'):
            with self.subTest(text=text):
                self.assertEqual(self.ns['_parse_document_list'](text), [])

    def test_unfenced_prose_and_trailing_explanation_are_accepted(self):
        docs = [{'title': 'HVAC', 'link': '/book/123'}]
        text = 'Here are the matching documents: ' + json.dumps(docs) + '\nI hope this helps.'
        self.assertEqual(self.ns['extract_json_from_text'](text), docs)

    def test_nested_arrays_and_brackets_in_strings_are_not_truncated(self):
        docs = [{'title': 'ASHRAE [HVAC]', 'details': [{'notes': 'literal } ] and \\"quotes\\"'}]}]
        self.assertEqual(self.ns['extract_json_from_text']('Answer: ' + json.dumps(docs) + ' Done.'), docs)

    def test_invalid_candidate_before_valid_array_is_skipped(self):
        self.assertEqual(self.ns['extract_json_from_text']('First [{bad}] then [{"title":"HVAC"}]'), [{'title': 'HVAC'}])


class StatusBridgeTests(unittest.IsolatedAsyncioTestCase):
    async def test_worker_notice_is_delivered_on_ui_thread_before_completion(self):
        ns, _, _ = load_evaluator([])
        released = threading.Event()
        main_thread = threading.get_ident()
        notices = []

        def invoke(state):
            self.assertNotEqual(threading.get_ident(), main_thread)
            ns['_report_status']('Switching provider')
            if not released.wait(3):
                raise RuntimeError('UI notice was not delivered while evaluation was running')
            return state

        def on_status(message):
            self.assertEqual(threading.get_ident(), main_thread)
            notices.append(message)
            released.set()

        ns['scraper_graph'] = SimpleNamespace(invoke=invoke)
        result = await ns['invoke_scraper_graph'](STATE, on_status)
        self.assertEqual(result, STATE)
        self.assertEqual(notices, ['Switching provider'])
        self.assertIsNone(ns['_status_messages'].get())


if __name__ == '__main__':
    unittest.main()
