from pathlib import Path
import subprocess
import tempfile

from hat import json
from hat import sbs
from hat.doit import common
from hat.doit.docs import (build_sphinx,
                           build_pdoc)
from hat.doit.js import (ESLintConf,
                         run_eslint)
from hat.doit.py import (get_task_build_wheel,
                         get_task_run_pytest,
                         get_task_run_pip_compile,
                         run_flake8)


__all__ = ['task_clean_all',
           'task_node_modules',
           'task_build',
           'task_check',
           'task_test',
           'task_create_ui_dir',
           'task_docs',
           'task_ui',
           'task_json_schema_repo',
           'task_sbs_repo',
           'task_pip_compile']


build_dir = Path('build')
src_py_dir = Path('src_py')
src_js_dir = Path('src_js')
src_static_dir = Path('src_static')
pytest_dir = Path('test_pytest')
docs_dir = Path('docs')
schemas_json_dir = Path('schemas_json')
schemas_sbs_dir = Path('schemas_sbs')
node_modules_dir = Path('node_modules')

build_py_dir = build_dir / 'py'
build_docs_dir = build_dir / 'docs'

ui_dir = src_py_dir / 'hat/monitor/server/ui'
json_schema_repo_path = src_py_dir / 'hat/monitor/json_schema_repo.json'
sbs_repo_path = src_py_dir / 'hat/monitor/sbs_repo.json'


def task_clean_all():
    """Clean all"""
    return {'actions': [(common.rm_rf, [build_dir,
                                        ui_dir,
                                        json_schema_repo_path,
                                        sbs_repo_path])]}


def task_node_modules():
    """Install node_modules"""
    return {'actions': ['yarn install --silent']}


def task_build():
    """Build"""
    return get_task_build_wheel(
        src_dir=src_py_dir,
        build_dir=build_py_dir,
        scripts={'hat-monitor': 'hat.monitor.server.main:main'},
        task_dep=['ui',
                  'json_schema_repo',
                  'sbs_repo'])


def task_check():
    """Check"""
    return {'actions': [(run_flake8, [src_py_dir]),
                        (run_flake8, [pytest_dir]),
                        (run_eslint, [src_js_dir, ESLintConf.TS])],
            'task_dep': ['node_modules']}


def task_test():
    """Test"""
    return get_task_run_pytest(task_dep=['json_schema_repo',
                                         'sbs_repo',
                                         'create_ui_dir'])


def task_create_ui_dir():
    """Create empty ui directory"""
    return {'actions': [(common.mkdir_p, [ui_dir])]}


def task_docs():
    """Docs"""

    def build():
        build_sphinx(src_dir=docs_dir,
                     dst_dir=build_docs_dir,
                     project='hat-monitor',
                     extensions=['sphinx.ext.graphviz',
                                 'sphinxcontrib.programoutput'])

        build_pdoc(module='hat.monitor',
                   dst_dir=build_docs_dir / 'py_api')

    return {'actions': [build],
            'task_dep': ['ui',
                         'json_schema_repo',
                         'sbs_repo']}


def task_ui():
    """Build UI"""

    def build(args):
        args = args or []
        common.rm_rf(ui_dir)
        common.cp_r(src_static_dir, ui_dir)
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            config_path = tmpdir / 'webpack.config.js'
            config_path.write_text(_webpack_conf.format(
                src_path=(src_js_dir / 'main.ts').resolve(),
                dst_dir=ui_dir.resolve()))
            subprocess.run([str(node_modules_dir / '.bin/webpack'),
                            '--config', str(config_path),
                            *args],
                           check=True)

    return {'actions': [build],
            'pos_arg': 'args',
            'task_dep': ['node_modules']}


def task_json_schema_repo():
    """Generate JSON Schema Repository"""
    src_paths = list(schemas_json_dir.rglob('*.yaml'))

    def generate():
        repo = json.SchemaRepository(*src_paths)
        data = repo.to_json()
        json.encode_file(data, json_schema_repo_path, indent=None)

    return {'actions': [generate],
            'file_dep': src_paths,
            'targets': [json_schema_repo_path]}


def task_sbs_repo():
    """Generate SBS repository"""
    src_paths = list(schemas_sbs_dir.rglob('*.sbs'))

    def generate():
        repo = sbs.Repository(*src_paths)
        data = repo.to_json()
        json.encode_file(data, sbs_repo_path, indent=None)

    return {'actions': [generate],
            'file_dep': src_paths,
            'targets': [sbs_repo_path]}


def task_pip_compile():
    """Run pip-compile"""
    return get_task_run_pip_compile()


_webpack_conf = r"""
module.exports = {{
    mode: 'none',
    entry: '{src_path}',
    output: {{
        filename: 'main.js',
        path: '{dst_dir}'
    }},
    module: {{
        rules: [
            {{
                test: /\.scss$/,
                use: [
                    "style-loader",
                    {{
                        loader: "css-loader",
                        options: {{url: false}}
                    }},
                    {{
                        loader: "sass-loader",
                        options: {{sourceMap: true}}
                    }}
                ]
            }},
            {{
                test: /\.ts$/,
                use: 'ts-loader'
            }}
        ]
    }},
    resolve: {{
        extensions: ['.ts', '.js']
    }},
    watchOptions: {{
        ignored: /node_modules/
    }},
    devtool: 'source-map',
    stats: 'errors-only'
}};
"""
