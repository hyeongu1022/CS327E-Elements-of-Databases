import datetime, logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import WriteToText
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery


# ******************************************** REMOVE DUPLICATES ************************************************************
class NoDuplicates(beam.DoFn):
  def process(self, element):
    employer_id = element['employer_id']
    employer_name = element['employer_name']
    employer_address = element['employer_address']
    employer_city = element['employer_city']
    employer_state = element['employer_state']
    employer_postal_code = element['employer_postal_code']
    employer_country = element['employer_country']
    employer_province = element['employer_province']
    h_1b_dependent = element['h_1b_dependent']
    willful_violator = element['willful_violator']
    
    record = {'employer_id':employer_id, 'employer_name': employer_name, 'employer_address': employer_address, 'employer_city': employer_city, 'employer_state': employer_state, 'employer_postal_code': employer_postal_code, 'employer_country': employer_country, 'employer_province': employer_province, 'h_1b_dependent': h_1b_dependent, 'willful_violator': willful_violator}
    return [record]



def run():
     PROJECT_ID = 'acquired-rarity-288205'
     BUCKET = 'gs://ykdb_beam_us'
     DIR_PATH = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'

     options = PipelineOptions(
     flags=None,
     runner='DataflowRunner',
     project=PROJECT_ID,
     job_name='employer',
     temp_location=BUCKET + '/temp',
     region='us-central1')
    
     p = beam.pipeline.Pipeline(options=options)
        
     
     # ***************************************** REMOVE DUPLICATES ****************************************************
     sql = "SELECT FARM_FINGERPRINT(TO_JSON_STRING(t)) AS employer_id, * FROM (SELECT employer_name, employer_address, employer_city, employer_state, employer_postal_code, employer_country, employer_province, h_1b_dependent, willful_violator FROM (SELECT  *, COUNT(*) AS count FROM H_1B_refined.Employer GROUP BY employer_name, employer_address, employer_city, employer_state, employer_postal_code, employer_country, employer_province, h_1b_dependent, willful_violator HAVING count = 1)) AS t"
     bq_source = ReadFromBigQuery(query=sql, use_standard_sql=True, gcs_location=BUCKET)

     query_results = p | 'Read from BQ' >> beam.io.Read(bq_source)
     
     out_pcoll = query_results | 'Remove Dups Employer' >> beam.ParDo(NoDuplicates())
     
     out_pcoll | 'Log output' >> WriteToText(DIR_PATH + 'output_employer.txt')
     
    
     # ***************************************** INSERT INTO BQ ****************************************************
     dataset_id = 'H_1B_refined'
     table_id = PROJECT_ID + ':' + dataset_id + '.' + 'Employer_Dataflow'
    
     schema_id = 'employer_id:INTEGER, employer_name:STRING, employer_address:STRING, employer_city:STRING, employer_state:STRING, employer_postal_code:STRING, employer_country:STRING, employer_province:STRING, h_1b_dependent:BOOLEAN, willful_violator:BOOLEAN'

     out_pcoll | 'Write to BQ' >> WriteToBigQuery(table=table_id, schema=schema_id, custom_gcs_temp_location=BUCKET)
     
     result = p.run()
     result.wait_until_finish()      

    
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.ERROR)
  run()