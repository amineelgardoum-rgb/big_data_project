from src.ingestion.ingest import ingest
from src.data_generation.generate_data import generate_data
from src.data_generation.utils.log import log
from src.ingestion.utils.date import DATE



def pipeline_create_ingest():
    log(f"Generating data for DATE :{DATE}","CREATE")
    generate_data()
    log(f"Begin the ingestion for DATE:{DATE}","INFO")
    ingest()
    

if __name__=="__main__":
    pipeline_create_ingest()