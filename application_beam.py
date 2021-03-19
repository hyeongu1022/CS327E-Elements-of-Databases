import logging, re
import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery


# ******************************************** REMOVE DUPLICATES ************************************************************
class NoDuplicates(beam.DoFn):
  def process(self, element):
    case_number = element['CASE_NUMBER']
    case_status = element['CASE_STATUS']
    case_submitted = element['CASE_SUBMITTED']
    decision_date = element['DECESION_DATE']
    visa_class = element['VISA_CLASS']
    employer_name = element['employer_name']
    employer_city = element['employer_city']
    
    record = {'CASE_NUMBER': case_number, 'CASE_STATUS': case_status, 'CASE_SUBMITTED': case_submitted, 'DECESION_DATE': decision_date, 'VISA_CLASS': visa_class, 'employer_name': employer_name, 'employer_city': employer_city}
    return [record]



def run():
     PROJECT_ID = 'acquired-rarity-288205'
     BUCKET = 'gs://ykdb_beam/temp'

     options = {
     'project': PROJECT_ID
     }
     opts = beam.pipeline.PipelineOptions(flags=[], **options)

     p = beam.Pipeline('DirectRunner', options=opts)
        
     
     # ***************************************** REMOVE DUPLICATES ****************************************************
     sql = "SELECT * FROM (SELECT  *, COUNT(*) AS count FROM H_1B_refined.Application GROUP BY CASE_NUMBER, CASE_STATUS, CASE_SUBMITTED, DECESION_DATE, VISA_CLASS, employer_name, employer_city HAVING count = 1) LIMIT 100"
     bq_source = ReadFromBigQuery(query=sql, use_standard_sql=True, gcs_location=BUCKET)

     query_results = p | 'Read from BQ' >> beam.io.Read(bq_source)
     
     out_pcoll = query_results | 'Remove Dups Application' >> beam.ParDo(NoDuplicates())
     
     out_pcoll | 'Log output' >> WriteToText('output_application.txt')
     
    
     # ***************************************** INSERT INTO BQ ****************************************************
     dataset_id = 'H_1B_refined'
     table_id = PROJECT_ID + ':' + dataset_id + '.' + 'Application_Beam'
    
     schema_id = 'CASE_NUMBER:STRING, CASE_STATUS:STRING, CASE_SUBMITTED:DATE, DECESION_DATE:DATE, VISA_CLASS:STRING, employer_name:STRING, employer_city:STRING'

     out_pcoll | 'Write to BQ' >> WriteToBigQuery(table=table_id, schema=schema_id, custom_gcs_temp_location=BUCKET)
     
     result = p.run()
     result.wait_until_finish()      

    
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.ERROR)
  run()