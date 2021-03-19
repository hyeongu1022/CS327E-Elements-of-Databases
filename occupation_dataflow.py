import logging, datetime
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import WriteToText
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery
'''
class FormatDate(beam.DoFn):
  def process(self, element):
    job_id = element['job_id']
    job_title = element['job_title']
    employer_id = element['employer_id']
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
    
    
    if ("." in  employment_start_date or "/" in employment_start_date):
        # handle YYYY.MM.DD 
        if "." in employment_start_date:
            split_start_date = employment_start_date.split('.') # yyyy.mm.dd -> [yyyy, mm, dd]
            year = split_start_date[0]
            month = split_start_date[1]
            day = split_start_date[2]
            if len(month) == 1:
                month = "0"+month
            if len(day) == 1:
                day = "0"+day
            employment_start_date = year + '-' + month + '-' + day
                    

        # handle MM/DD/YYYY 
        elif "/" in employment_start_date:
            split_start_date = employment_start_date.split('/') # mm/dd/yyyy -> [yyyy, mm, dd]
            year = split_start_date[2]
            month = split_start_date[0]
            day = split_start_date[1]
            if len(month) == 1:
                month = "0"+month
            if len(day) == 1:
                day = "0"+day
            employment_start_date = year + '-' + month + '-' + day
    else: #if 2005-1-2 -> 2005-01-02
        split_start_date = employment_start_date.split('-') # yyyy-mm-dd -> [yyyy, mm, dd]
        year = split_start_date[0]
        month = split_start_date[1]
        day = split_start_date[2]
        if len(month) == 1:
            month = "0"+month
        if len(day) == 1:
            day = "0"+day
        employment_start_date = year + '-' + month + '-' + day
    

    
    if ("." in  employment_end_date or "/" in employment_end_date):
         # handle YYYY.MM.DD 
        if "." in employment_end_date:
            split_end_date = employment_end_date.split('.') # yyyy.mm.dd -> [yyyy, mm, dd]
            year = split_end_date[0]
            month = split_end_date[1]
            day = split_end_date[2]
            if len(month) == 1:
                month = "0"+month
            if len(day) == 1:
                day = "0"+day
            employment_end_date = year + '-' + month + '-' + day    

            # handle MM/DD/YYYY 
        elif "/" in employment_end_date:
            split_end_date = employment_end_date.split('/') # mm/dd/yyyy -> [yyyy, mm, dd]
            year = split_end_date[2]
            month = split_end_date[0]
            day = split_end_date[1]
            if len(month) == 1:
                month = "0"+month
            if len(day) == 1:
                day = "0"+day
            employment_end_date = year + '-' + month + '-' + day    
            
    else: #if 2005-1-2 -> 2005-01-02
        split_end_date = employment_end_date.split('-') # yyyy-mm-dd -> [yyyy, mm, dd]
        year = split_end_date[0]
        month = split_end_date[1]
        day = split_end_date[2]
        if len(month) == 1:
            month = "0"+month
        if len(day) == 1:
            day = "0"+day
        employment_end_date = year + '-' + month + '-' + day
   
    
    record = {'job_id':job_id, 'job_title': job_title, 'employer_id':employer_id, 'employment_start_date': employment_start_date, 'employment_end_date': employment_end_date, 'soc_code': soc_code, 'soc_title': soc_title, 'prevailing_wage_YR': prevailing_wage_YR, 'pw_wage_level': pw_wage_level, 'pw_wage_source': pw_wage_source, 'pw_wage_source_year': pw_wage_source_year, 'pw_wage_source_other': pw_wage_source_other, 'worksite_city': worksite_city, 'worksite_country': worksite_country, 'worksite_state': worksite_state, 'worksite_postal_code': worksite_postal_code}
    return [record]
    '''
class FormatSocCode(beam.DoFn):
  def process(self, element):  
    job_id = element['job_id']
    job_title = element['job_title']
    employer_id = element['employer_id']
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
    
    if "-" in soc_code:
        if len(soc_code) == 6:
            soc_code = None
        elif len(soc_code) == 7:
            soc_code = soc_code
        else: # 23-1011.00
            soc_code = soc_code[:7]
            
    elif "." in soc_code:
        soc_code = soc_code.replace(".", "") # 12.3456.00 -> 12345600
        if len(soc_code) == 6:
            soc_code = soc_code[:2] + "-" + soc_code[2:]
        elif len(soc_code) > 7: 
            soc_code = soc_code[:2] + "-" + soc_code[2:6]
        else: # soc_code < 6
            soc_code = None
            
    elif len(soc_code) == 7 and "/" in soc_code:
            soc_code = soc_code.replace("/", "-")
    else:
        if len(soc_code) == 6:
            soc_code = soc_code[:2] + "-" + soc_code[2:]
        else:
            soc_code = None
        
            
    
    record = {'job_id':job_id, 'job_title': job_title, 'employer_id':employer_id, 'soc_code': soc_code, 'soc_title': soc_title, 'prevailing_wage_YR': prevailing_wage_YR, 'pw_wage_level': pw_wage_level, 'pw_wage_source': pw_wage_source, 'pw_wage_source_year': pw_wage_source_year, 'pw_wage_source_other': pw_wage_source_other, 'worksite_city': worksite_city, 'worksite_country': worksite_country, 'worksite_state': worksite_state, 'worksite_postal_code': worksite_postal_code}
    return [record]



def run():
     PROJECT_ID = 'acquired-rarity-288205'
     BUCKET = 'gs://ykdb_beam_us'
     DIR_PATH = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'

     options = PipelineOptions(
     flags=None,
     runner='DataflowRunner',
     project=PROJECT_ID,
     job_name='occupation',
     temp_location=BUCKET + '/temp',
     region='us-central1')
    
     p = beam.pipeline.Pipeline(options=options)
      
     '''
     sql = "SELECT FARM_FINGERPRINT(TO_JSON_STRING(t)) AS job_id, * FROM (SELECT job_title, emp.employer_id AS employer_id, soc_code, soc_title, prevailing_wage_YR, pw_wage_level, pw_wage_source, pw_wage_source_year, pw_wage_source_other, worksite_city, worksite_country, worksite_state, worksite_postal_code FROM (SELECT  *, COUNT(*) AS count FROM H_1B_refined.Occupation WHERE prevailing_wage_YR > 5000 GROUP BY job_title, employer_name, employer_city, employment_start_date, employment_end_date, soc_code, soc_title, prevailing_wage_YR, pw_wage_level, pw_wage_source, pw_wage_source_year, pw_wage_source_other, worksite_city, worksite_country, worksite_state, worksite_postal_code HAVING count = 1) AS occ JOIN H_1B_refined.Employer_Dataflow AS emp ON emp.employer_name = occ.employer_name AND emp.employer_city = occ.employer_city) AS t WHERE length(soc_code) >= 6 AND length(soc_code) <= 10 AND length(soc_code) != 8" '''
    
     sql="SELECT FARM_FINGERPRINT(TO_JSON_STRING(t)) AS job_id, job_title, employer_id, soc_code, soc_title, prevailing_wage_YR, pw_wage_level, pw_wage_source, pw_wage_source_year, pw_wage_source_other, worksite_city, worksite_country, worksite_state, worksite_postal_code FROM (SELECT  *, COUNT(*) AS count FROM(SELECT job_title, emp.employer_id AS employer_id, occ.employer_name, occ.employer_city, employment_start_date, employment_end_date, soc_code, soc_title, prevailing_wage_YR, pw_wage_level, pw_wage_source, pw_wage_source_year, pw_wage_source_other, worksite_city, worksite_country, worksite_state, worksite_postal_code FROM H_1B_refined.Occupation as occ JOIN H_1B_refined.Employer_Dataflow AS emp ON emp.employer_name = occ.employer_name AND emp.employer_city = occ.employer_city WHERE prevailing_wage_YR > 5000 AND length(soc_code) >= 6 AND length(soc_code) <= 10 AND length(soc_code) != 8) GROUP BY job_title, employer_id, employer_name, employer_city, employment_start_date, employment_end_date, soc_code, soc_title, prevailing_wage_YR, pw_wage_level, pw_wage_source, pw_wage_source_year, pw_wage_source_other, worksite_city, worksite_country, worksite_state, worksite_postal_code HAVING count = 1) as t"
        
     bq_source = ReadFromBigQuery(query=sql, use_standard_sql=True, gcs_location=BUCKET)

     query_results = p | 'Read from BQ' >> beam.io.Read(bq_source)
     '''
     out_pcoll_fix_date = query_results | 'Format Date' >> beam.ParDo(FormatDate())
        
     out_pcoll_fix_date | 'Log fix_date_output' >> WriteToText(DIR_PATH + 'output_occ_fix_date.txt')
     '''
     out_pcoll = query_results | 'Format Soc' >> beam.ParDo(FormatSocCode())
     
     out_pcoll | 'Log output' >> WriteToText(DIR_PATH + 'output_occupation.txt')
     
        
     # ***************************************** INSERT INTO BQ ****************************************************
     dataset_id = 'H_1B_refined'
     table_id = PROJECT_ID + ':' + dataset_id + '.' + 'Occupation_Dataflow'
    
     schema_id = 'job_id:INTEGER, job_title:STRING, employer_id:INTEGER, soc_code:STRING, soc_title:STRING, prevailing_wage_YR:FLOAT, pw_wage_level:STRING, pw_wage_source:STRING, pw_wage_source_year:INTEGER, pw_wage_source_other:STRING, worksite_city:STRING, worksite_country:STRING, worksite_state:STRING, worksite_postal_code:STRING'
     
     out_pcoll | 'Write to BQ' >> WriteToBigQuery(table=table_id, schema=schema_id, custom_gcs_temp_location=BUCKET)
     
     result = p.run()
     result.wait_until_finish()      


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.ERROR)
  run()