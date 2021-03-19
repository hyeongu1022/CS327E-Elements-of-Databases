import datetime, logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import WriteToText
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery


# ******************************************** REMOVE DUPLICATES ************************************************************
class NoDuplicates(beam.DoFn):
  def process(self, element):
    ownership_id = element['ownership_id']
    occ_code = element['occ_code']
    occ_title = element['occ_title']
    ownership = element['ownership']
    naics_title = element['naics_title']
    grp = element['grp']
    tot_emp = element['tot_emp']
    emp_prse = element['emp_prse']
    h_mean = element['h_mean']
    a_mean = element['a_mean']
    mean_prse = element['mean_prse']
    a_pct10 = element['a_pct10']
    a_pct25 = element['a_pct25']
    a_median = element['a_median']
    a_pct75 = element['a_pct75']
    a_pct90 = element['a_pct90']
    
    record = {'ownership_id':ownership_id, 'occ_code': occ_code, 'occ_title': occ_title, 'ownership': ownership, 'naics_title': naics_title, 'grp': grp, 'tot_emp': tot_emp, 'emp_prse': emp_prse, 'h_mean': h_mean, 'a_mean': a_mean, 'mean_prse': mean_prse, 'a_pct10': a_pct10, 'a_pct25': a_pct25, 'a_median': a_median, 'a_pct75': a_pct75, 'a_pct90': a_pct90}
    return [record]



def run():
     PROJECT_ID = 'acquired-rarity-288205'
     BUCKET = 'gs://ykdb_beam_us'
     DIR_PATH = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'

     options = PipelineOptions(
     flags=None,
     runner='DataflowRunner',
     project=PROJECT_ID,
     job_name='ownership',
     temp_location=BUCKET + '/temp',
     region='us-central1')
    
     p = beam.pipeline.Pipeline(options=options)
        
     
     # ***************************************** REMOVE DUPLICATES ****************************************************
     sql = "SELECT FARM_FINGERPRINT(TO_JSON_STRING(t)) as ownership_id, * FROM (SELECT * FROM (SELECT  *, COUNT(*) AS count FROM H_1B_refined.Ownership GROUP BY occ_code, occ_title, ownership, naics_title, grp, tot_emp, emp_prse, h_mean, a_mean, mean_prse, a_pct10, a_pct25, a_median, a_pct75, a_pct90 HAVING count = 1)) as  t"
     bq_source = ReadFromBigQuery(query=sql, use_standard_sql=True, gcs_location=BUCKET)

     query_results = p | 'Read from BQ' >> beam.io.Read(bq_source)
     
     out_pcoll = query_results | 'Remove Dups Ownership' >> beam.ParDo(NoDuplicates())
     
     out_pcoll | 'Log output' >> WriteToText(DIR_PATH + 'output_ownership.txt')
     
    
     # ***************************************** INSERT INTO BQ ****************************************************
     dataset_id = 'H_1B_refined'
     table_id = PROJECT_ID + ':' + dataset_id + '.' + 'Ownership_Dataflow'
    
     schema_id = 'ownership_id:INTEGER, occ_code:STRING, occ_title:STRING, ownership:STRING, naics_title:STRING, grp:STRING, tot_emp:INTEGER, emp_prse:FLOAT, h_mean:FLOAT, a_mean:INTEGER, mean_prse:FLOAT, a_pct10:INTEGER, a_pct25:INTEGER, a_median:INTEGER, a_pct75:INTEGER, a_pct90:INTEGER'

     out_pcoll | 'Write to BQ' >> WriteToBigQuery(table=table_id, schema=schema_id, custom_gcs_temp_location=BUCKET)
     
     result = p.run()
     result.wait_until_finish()      

    
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.ERROR)
  run()