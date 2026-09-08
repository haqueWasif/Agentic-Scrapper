"""Build the Colab upload bundle solely from this version's source folder."""
import ast
import json
from pathlib import Path
import shutil
import zipfile

HERE = Path(__file__).resolve().parent
SOURCE = HERE / 'source'
NOTEBOOK = 'Agentic_Scraper_Colab.ipynb'


def build():
    files = [SOURCE / name for name in ('app.py', 'requirements.txt', 'config/settings.yaml')]
    for folder in ('app', 'tests', 'colab'):
        files.extend(path for path in (SOURCE / folder).rglob('*')
                     if path.is_file() and path.suffix in ('.py', '.md', '.sql', '.txt', '.ipynb')
                     and '__pycache__' not in path.parts)
    for path in files:
        if path.suffix == '.py':
            compile(path.read_text(encoding='utf-8-sig'), str(path), 'exec')
    notebook_path = SOURCE / 'colab' / NOTEBOOK
    notebook = json.loads(notebook_path.read_text(encoding='utf-8'))
    for cell in notebook['cells']:
        if cell['cell_type'] == 'code':
            compile(''.join(cell['source']), '<notebook>', 'exec', flags=ast.PyCF_ALLOW_TOP_LEVEL_AWAIT)
    shutil.copy2(notebook_path, HERE / NOTEBOOK)
    bundle = HERE / 'Agentic_Scraper_Colab_source.zip'
    with zipfile.ZipFile(bundle, 'w', zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(set(files)):
            archive.write(path, path.relative_to(SOURCE).as_posix())
    with zipfile.ZipFile(bundle) as archive:
        assert archive.testzip() is None
        assert 'colab/distributed_schema.sql' in archive.namelist()
        assert 'colab/requirements-distributed.txt' in archive.namelist()
        assert all(not name.startswith(('runtime/', 'data/', '.env')) for name in archive.namelist())
    print(f'Built {bundle.name}: {len(set(files))} source files; notebook cells compile.')
    return bundle


if __name__ == '__main__':
    build()
