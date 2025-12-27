import sys
import os

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, BASE_DIR)

def test_import_pipeline_modules():
    import scripts.pipeline_orchestrator
    import scripts.scheduler
    import scripts.cleanup_old_data
