import logging, re
import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery

# ******************************************** SOC_CODE FIX ************************************************************
class FormatSocCode(beam.DoFn):
  def process(self, element):  
    job_title = element['job_title']
    employer_name = element['employer_name']
    employer_city = element['employer_city']
    employment_start_date = element['employment_start_date']
    employment_end_date = element['employment_end_date']
    soc_code = element['soc_code']
    soc_title = element['soc_title']
    prevailing_wage_YR = element['prevailing_wage_YR']
    pw_wage_level = element['pw_wage_level']
    pw_wage_source = element['pw_wage_source']
    pw_wage_source_year = element['pw_wage_source_year']
    pw_wage_source_other = element['pw_wage_source_other']
    worksite_city = element['worksite_city']
    worksite_country = element['worksite_country']
    worksite_state = element['worksite_state']
    worksite_postal_code = element['worksite_postal_code']
    
    # if the element is not NULL
    if soc_code != None and len(soc_code) <= 8:  # some are letters like "Computer System Analyst"

        if "." in soc_code: # make all 11.1111 or 11.1111.00 formats to 111111
            soc_code = soc_code.replace(".", "")
        if len(soc_code) == 6: #those with 123456 format
            new_soc_code = soc_code[:2] + "-" + soc_code[2:]
            print('input soc_code: ' + element['soc_code'] + ', output soc_code: ' + new_soc_code)
            soc_code = new_soc_code
        elif len(soc_code) > 7: #those with 12-3456.00 format
            new_soc_code = soc_code[:2] + "-" + soc_code[2:6]
            print('input soc_code: ' + element['soc_code'] + ', output soc_code: ' + new_soc_code)
            soc_code = new_soc_code
    # for those with Null and integer, just leave it
    
    record = {'job_title': job_title, 'employer_name': employer_name, 'employer_city': employer_city, 'employment_start_date': employment_start_date, 'employment_end_date': employment_end_date, 'soc_code': soc_code, 'soc_title': soc_title, 'prevailing_wage_YR': prevailing_wage_YR, 'pw_wage_level': pw_wage_level, 'pw_wage_source': pw_wage_source, 'pw_wage_source_year': pw_wage_source_year, 'pw_wage_source_other': pw_wage_source_other, 'worksite_city': worksite_city, 'worksite_country': worksite_country, 'worksite_state': worksite_state, 'worksite_postal_code': worksite_postal_code}
    return [record]



def run():
     PROJECT_ID = 'acquired-rarity-288205'
     BUCKET = 'gs://ykdb_beam/temp'

     options = {
     'project': PROJECT_ID
     }
     opts = beam.pipeline.PipelineOptions(flags=[], **options)

     p = beam.Pipeline('DirectRunner', options=opts)
        
     
     # ***************************************** FIXING soc_code ****************************************************
     sql = "SELECT * FROM H_1B_refined.Occupation_fix_date WHERE length(soc_code) != 7 AND soc_code NOT LIKE '%-%' LIMIT 200"
     bq_source = ReadFromBigQuery(query=sql, use_standard_sql=True, gcs_location=BUCKET)
     
     query_results = p | 'Read from BQ' >> beam.io.Read(bq_source)
     
     out_pcoll = query_results | 'Format Soc' >> beam.ParDo(FormatSocCode())
     
     out_pcoll | 'Log output' >> WriteToText('output_occupation.txt')
     
    
     # ***************************************** INSERT INTO BQ ****************************************************
     dataset_id = 'H_1B_refined'
     table_id = PROJECT_ID + ':' + dataset_id + '.' + 'Occupation_Beam'
     schema_id = 'job_title:STRING, employer_name:STRING, employer_city:STRING, employment_start_date:DATE, employment_end_date:DATE, soc_code:STRING, soc_title:STRING, prevailing_wage_YR:FLOAT, pw_wage_level:STRING, pw_wage_source:STRING, pw_wage_source_year:INTEGER, pw_wage_source_other:STRING, worksite_city:STRING, worksite_country:STRING, worksite_state:STRING, worksite_postal_code:STRING'

     out_pcoll | 'Write to BQ' >> WriteToBigQuery(table=table_id, schema=schema_id, custom_gcs_temp_location=BUCKET)
     
     result = p.run()
     result.wait_until_finish()      

    
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.ERROR)
  run()