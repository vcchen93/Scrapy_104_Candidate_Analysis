from Scrapy_Analysis.items import ScrapyAnalysisItem
import scrapy
import scrapy_proxies
import pandas as pd
import json
import time
import ast


class get_Analysis_Spider(scrapy.Spider):
    name = 'analysis_104'

    def start_requests(self):
        
        df_all = pd.read_csv(r'C:\Users\user\Desktop\Project_Data\df_codes_both.csv', delimiter="\t")
        
        header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36'}
        
        cookie_login = {
                'AC':'1594371894',
                'JBCLOGIN':'dOguhH4IMDSNxpISp_Lqm7Y4MHbxJKQ55aVnuIY7W8U',
                'EPK':'134279daa9933e0cea8d82ac7f76692fd',
            'JBCLOGIN.sig':'862VpDpcwHRqYPY1PiEU5df41Gc'
            }
        
        
        for i in df_all.code_dic:
            for j in ast.literal_eval(i).values():
               
                yield scrapy.Request(url='https://www.104.com.tw/jb/104i/applyAnalysisToJob/all?job_no={}'.format(j), cookies = cookie_login, callback=self.parse)
                time.sleep(0.1)
    
    
    
    def parse(self, response):

        item = ScrapyAnalysisItem()
        
        list_ans_json = json.loads(response.text)      
        
    
    
    
    
    
    
        def sumAge():
            if total != 0:
                sumAgeUse = 0
                for a in range(0,10):
                    sumAgeUse += int(list_ans_json['yearRange'][str(a)]['count']) * (18+a*5)
                return(round(sumAgeUse/total,1))

            else:
                return('0')

        def sumExp():
            if total != 0:
                return(
                    round(((int(list_ans_json['exp']['1']['count']) * 0.5)+  \
                    (int(list_ans_json['exp']['2']['count']) * 2)+  \
                    (int(list_ans_json['exp']['3']['count']) * 4)+  \
                    (int(list_ans_json['exp']['4']['count']) * 8)+  \
                    (int(list_ans_json['exp']['5']['count']) * 13)+  \
                    (int(list_ans_json['exp']['6']['count']) * 18)+  \
                    (int(list_ans_json['exp']['7']['count']) * 23)+  \
                    (int(list_ans_json['exp']['8']['count']) * 27))  \
                        /total,1)
                    )
            else:
                return('0')

        def commLanguage():
            comlang = ''

            for lang in list_ans_json['language']:
                if len(lang) <= 3:
                    if float(list_ans_json['language'][lang]['percent'])>25:
                        comlang += (list_ans_json['language'][lang]['langName']+' ')
            return(comlang)

        def commMajor():
            comMaj = ''

            for maj in list_ans_json['major']:
                if len(maj) <= 3:
                    if float(list_ans_json['major'][maj]['percent'])>25:
                        comMaj += (list_ans_json['major'][maj]['majorName']+' ')
            return(comMaj)

        def commSkill():
            comskl = ''

            for skl in list_ans_json['skill']:
                if len(skl) <= 3:
                    if float(list_ans_json['skill'][skl]['percent'])>25:
                        comskl += (list_ans_json['skill'][skl]['skillName']+' ')
            return(comskl)

        def commCert():
            comcer = ''

            for cer in list_ans_json['cert']:
                if len(cer) <= 3:
                    if float(list_ans_json['cert'][cer]['percent'])>25:
                        comcer += (list_ans_json['cert'][cer]['certName']+' ')
            return(comcer)
       
        
        total = int(list_ans_json['sex']['total'])
        
        item['total_applied'] =   total
        item['sex_male'] =        list_ans_json['sex']['0']['count']
        item['sex_female'] =      list_ans_json['sex']['1']['count']
        item['edu_postgrad'] =    list_ans_json['edu']['0']['count']
        item['edu_grad'] =        list_ans_json['edu']['1']['count']
        item['edu_likeGrad'] =    list_ans_json['edu']['2']['count']
        item['edu_high'] =        list_ans_json['edu']['3']['count']
        item['edu_junior'] =      list_ans_json['edu']['4']['count']
        item['edu_na'] =          list_ans_json['edu']['5']['count']
        item['age_na'] =          list_ans_json['edu']['6']['count']
        item['age_20'] =          list_ans_json['yearRange']['0']['count']
        item['age_21_25'] =       list_ans_json['yearRange']['1']['count']
        item['age_26_30'] =       list_ans_json['yearRange']['2']['count']
        item['age_31_35'] =       list_ans_json['yearRange']['3']['count']
        item['age_36_40'] =       list_ans_json['yearRange']['4']['count']
        item['age_41_45'] =       list_ans_json['yearRange']['5']['count']
        item['age_46_50'] =       list_ans_json['yearRange']['6']['count']
        item['age_51_55'] =       list_ans_json['yearRange']['7']['count']
        item['age_56_60'] =       list_ans_json['yearRange']['8']['count']
        item['age_61'] =          list_ans_json['yearRange']['9']['count']
        item['age_avg'] =         sumAge()
        item['exp_na'] =          list_ans_json['exp']['0']['count']
        item['exp_1'] =           list_ans_json['exp']['1']['count']
        item['exp_1_3'] =         list_ans_json['exp']['2']['count']
        item['exp_3_5'] =         list_ans_json['exp']['3']['count']
        item['exp_5_10'] =        list_ans_json['exp']['4']['count']
        item['exp_10_15'] =       list_ans_json['exp']['5']['count']
        item['exp_15_20'] =       list_ans_json['exp']['6']['count']
        item['exp_20_25'] =       list_ans_json['exp']['7']['count']
        item['exp_26'] =          list_ans_json['exp']['8']['count']
        item['exp_avg'] =         sumExp()
        item['commLanguage'] =    commLanguage()
        item['commMajor'] =       commMajor()
        item['commSkill'] =       commSkill()
        item['commCert'] =        commCert()


        yield item
