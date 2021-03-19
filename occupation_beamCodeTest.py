import logging, re
import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery

# ******************************************** SOC_CODE FIX ************************************************************
class NoDuplicates(beam.DoFn):
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
    
    record = {'job_title': job_title, 'employer_name': employer_name, 'employer_city': employer_city, 'employment_start_date': employment_start_date, 'employment_end_date': employment_end_date, 'soc_code': soc_code, 'soc_title': soc_title, 'prevailing_wage_YR': prevailing_wage_YR, 'pw_wage_level': pw_wage_level, 'pw_wage_source': pw_wage_source, 'pw_wage_source_year': pw_wage_source_year, 'pw_wage_source_other': pw_wage_source_other, 'worksite_city': worksite_city, 'worksite_country': worksite_country, 'worksite_state': worksite_state, 'worksite_postal_code': worksite_postal_code}
    return [record]


class FormatDate(beam.DoFn):
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
    if ("." in  employment_start_date or "/" in employment_start_date) and employment_start_date != None:
        # handle YYYY.MM.DD 
        if "." in employment_start_date:
            split_start_date = employment_start_date.split('.') # yyyy.mm.dd -> [yyyy, mm, dd]
            year = split_start_date[0]
            month = split_start_date[1]
            day = split_start_date[2]

        # handle MM/DD/YYYY 
        elif "/" in employment_start_date:
            split_start_date = employment_start_date.split('/') # mm/dd/yyyy -> [yyyy, mm, dd]
            year = split_start_date[2]
            month = split_start_date[0]
            day = split_start_date[1]
            
        employment_start_date = year + '-' + month + '-' + day
        #print('input start_date: ' + element['employment_start_date'] + ', output start_date: ' + employment_start_date)
    
    if ("." in  employment_end_date or "/" in employment_end_date) and employment_end_date != None:
        # handle YYYY.MM.DD 
        if "." in employment_end_date:
            split_end_date = employment_end_date.split('.') # yyyy.mm.dd -> [yyyy, mm, dd]
            year = split_end_date[0]
            month = split_end_date[1]
            day = split_end_date[2]

        # handle MM/DD/YYYY 
        elif "/" in employment_end_date:
            split_end_date = employment_end_date.split('/') # mm/dd/yyyy -> [yyyy, mm, dd]
            year = split_end_date[2]
            month = split_end_date[0]
            day = split_end_date[1]
            
        employment_end_date = year + '-' + month + '-' + day
        #print('input end_date: ' + element['employment_end_date'] + ', output end_date: ' + employment_end_date)
        
    record = {'job_title': job_title, 'employer_name': employer_name, 'employer_city': employer_city, 'employment_start_date': employment_start_date, 'employment_end_date': employment_end_date, 'soc_code': soc_code, 'soc_title': soc_title, 'prevailing_wage_YR': prevailing_wage_YR, 'pw_wage_level': pw_wage_level, 'pw_wage_source': pw_wage_source, 'pw_wage_source_year': pw_wage_source_year, 'pw_wage_source_other': pw_wage_source_other, 'worksite_city': worksite_city, 'worksite_country': worksite_country, 'worksite_state': worksite_state, 'worksite_postal_code': worksite_postal_code}
    return [record]


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
    
    
    if "." in soc_code: # make all 11.1111 or 11.1111.00 formats to 111111
            soc_code = soc_code.replace(".", "")
            if "-" in soc_code: # like 11-1111.00 
                soc_code = soc_code[0:7]
    # if the element is not NULL
    if soc_code != None and soc_code != "N/A" and len(soc_code) <= 10 and len(soc_code) != 7 and "-" not in soc_code:  # some are letters like "Computer System Analyst"
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
        
     
     # ***************************************** REMOVE DUPLICATES ****************************************************
     sql = "SELECT job_title, employer_name, employer_city, employment_start_date, employment_end_date, soc_code, soc_title, prevailing_wage_YR, pw_wage_level, pw_wage_source, pw_wage_source_year, pw_wage_source_other, worksite_city, worksite_country, worksite_state, worksite_postal_code FROM (SELECT  *, COUNT(*) AS count FROM H_1B_refined.Occupation WHERE prevailing_wage_YR > 5000 AND length(soc_code) > 5 AND soc_code NOT LIKE '%-%' GROUP BY job_title, employer_name, employer_city, employment_start_date, employment_end_date, soc_code, soc_title, prevailing_wage_YR, pw_wage_level, pw_wage_source, pw_wage_source_year, pw_wage_source_other, worksite_city, worksite_country, worksite_state, worksite_postal_code HAVING count = 1) LIMIT 50"
     bq_source = ReadFromBigQuery(query=sql, use_standard_sql=True, gcs_location=BUCKET)

     query_results = p | 'Read from BQ' >> beam.io.Read(bq_source)
     
     out_pcoll_no_dup = query_results | 'Format prevailing_wage_YR and Remove dups' >> beam.ParDo(NoDuplicates())
    
     out_pcoll_fix_date = out_pcoll_no_dup | 'Format Date' >> beam.ParDo(FormatDate())
        
     out_pcoll = out_pcoll_fix_date | 'Format Soc' >> beam.ParDo(FormatSocCode())
     
     out_pcoll | 'Log output' >> WriteToText('output_occ_codeTest.txt')
     
    
     # ***************************************** INSERT INTO BQ ****************************************************
     dataset_id = 'H_1B_refined'
     table_id = PROJECT_ID + ':' + dataset_id + '.' + 'Occ_CodeTest'
     schema_id = 'job_title:STRING, employer_name:STRING, employer_city:STRING, employment_start_date:Date, employment_end_date:Date, soc_code:STRING, soc_title:STRING, prevailing_wage_YR:FLOAT, pw_wage_level:STRING, pw_wage_source:STRING, pw_wage_source_year:INTEGER, pw_wage_source_other:STRING, worksite_city:STRING, worksite_country:STRING, worksite_state:STRING, worksite_postal_code:STRING'

     out_pcoll | 'Write to BQ' >> WriteToBigQuery(table=table_id, schema=schema_id, custom_gcs_temp_location=BUCKET)
     
     result = p.run()
     result.wait_until_finish()      

    
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.ERROR)
  run()