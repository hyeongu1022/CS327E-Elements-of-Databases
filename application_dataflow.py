import datetime, logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import WriteToText
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery


# ******************************************** REMOVE DUPLICATES ************************************************************
class NoDuplicates(beam.DoFn):
  def process(self, element):
    case_id = element['case_id']
    employer_id = element['employer_id']
    case_number = element['CASE_NUMBER']
    case_status = element['CASE_STATUS']
    case_submitted = element['CASE_SUBMITTED']
    decision_date = element['DECISION_DATE']
    visa_class = element['VISA_CLASS']
    
    
    record = {'case_id': case_id, 'employer_id': employer_id, 'CASE_NUMBER': case_number, 'CASE_STATUS': case_status, 'CASE_SUBMITTED': case_submitted, 'DECISION_DATE': decision_date, 'VISA_CLASS': visa_class}
    return [record]



def run():
     PROJECT_ID = 'acquired-rarity-288205'
     BUCKET = 'gs://ykdb_beam_us'
     DIR_PATH = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'

     options = PipelineOptions(
     flags=None,
     runner='DataflowRunner',
     project=PROJECT_ID,
     job_name='application',
     temp_location=BUCKET + '/temp',
     region='us-central1')
    
     p = beam.pipeline.Pipeline(options=options)
        
     
     # ***************************************** REMOVE DUPLICATES ****************************************************
     sql = "SELECT FARM_FINGERPRINT(TO_JSON_STRING(t)) AS case_id, * FROM (SELECT emp.employer_id, CASE_NUMBER, CASE_STATUS, CASE_SUBMITTED, DECESION_DATE AS DECISION_DATE, VISA_CLASS FROM (SELECT  *, COUNT(*) AS count FROM H_1B_refined.Application GROUP BY CASE_NUMBER, CASE_STATUS, CASE_SUBMITTED, DECESION_DATE, VISA_CLASS, employer_name, employer_city HAVING count = 1) AS app JOIN H_1B_refined.Employer_Dataflow AS emp ON emp.employer_name = app.employer_name AND emp.employer_city = app.employer_city) AS t"
     bq_source = ReadFromBigQuery(query=sql, use_standard_sql=True, gcs_location=BUCKET)

     query_results = p | 'Read from BQ' >> beam.io.Read(bq_source)
     
     out_pcoll = query_results | 'Application Transfromation' >> beam.ParDo(NoDuplicates())
     
     out_pcoll | 'Log output' >> WriteToText(DIR_PATH + 'output_appplication.txt')
     
    
     # ***************************************** INSERT INTO BQ ****************************************************
     dataset_id = 'H_1B_refined'
     table_id = PROJECT_ID + ':' + dataset_id + '.' + 'Application_Dataflow'
    
     schema_id = 'case_id:INTEGER, employer_id:INTEGER, CASE_NUMBER:STRING, CASE_STATUS:STRING, CASE_SUBMITTED:DATE, DECISION_DATE:DATE, VISA_CLASS:STRING'

     out_pcoll | 'Write to BQ' >> WriteToBigQuery(table=table_id, schema=schema_id, custom_gcs_temp_location=BUCKET)
        
     
     result = p.run()
     result.wait_until_finish()      

    
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.ERROR)
  run()