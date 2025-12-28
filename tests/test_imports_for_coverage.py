import sys
import os

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, BASE_DIR)

def test_import_pipeline_modules():
    import scripts.pipeline_orchestrator
    import scripts.scheduler
    import scripts.cleanup_old_data
    import scripts.data_generation
    import scripts.ingestion
    import scripts.transformation.staging_to_production
    import scripts.transformation.load_warehouse
