from pymarc import Record,XMLWriter, TextWriter, MARCWriter
import pandas as pd
from pymarc.field import Field
from datetime import datetime
import re
import io
# df = pd.read_excel("data/Bibliografie_prekladu.xlsx", index_col=0)
# df = df.reset_index()  
# y_nan = pd.isnull(df.loc[:, df.columns != 'Číslo záznamu']).all(1).to_numpy().nonzero()[0]
# df = df.drop(df.index[y_nan]).copy(deep=True)
# df.to_csv('Bibliografie_prekladu.csv')

IN = 'Bibliografie_prekladu.csv'
OUT = 'data/marc_it.mrc'

df = pd.read_csv(IN, encoding='utf_8')


def create_008(row) -> str:
    date_record_creation = str(datetime.today().strftime('%y%m%d'))
    letter = 's'

    if pd.isnull(row['Rok']):
        publication_date = '--------'
    else:
        publication_date = str(int(row['Rok']))+ '----' 

    if pd.isnull(row['Město vydání, země vydání, nakladatel']):
        publication_country = 'xx-'

    else:
        publication = row['Město vydání, země vydání, nakladatel'] 
        start = publication.find('(')+1
        end = publication.find(')') 
        country = publication[start:end]
        if country == 'Itálie':
            publication_country = 'it-'
        elif country == 'Česká republika':
            publication_country = 'xr-'    
        else:
            publication_country = 'xx-'

    material_specific =  '-----------------'
    language = 'ita'
    modified = '-'
    cataloging_source = 'd'
    data = date_record_creation + letter + publication_date +  publication_country + material_specific + language + modified + cataloging_source
    return data

def add_595(record, row, author):
    if author is None:
        if not(pd.isnull(row['Původní název'])):  
            record.add_ordered_field(Field(tag='595', indicators = ['1', '2'], subfields = ['t', row['Původní název'] ,
                                                                                '1', str(record['001']) ])) 
        else: 
             record.add_ordered_field(Field(tag='595', indicators = ['1', '2'], subfields = ['1', str(record['001']) ]))                                                                        
    else:
        record.add_ordered_field(Field(tag='595', indicators = ['1', '2'], subfields = ['a', author, 
                                                                            't', row['Původní název'] ,
                                                                            '1', str(record['001']) ]))    


def add_author_code(data, record):
    if not(pd.isnull(data)):
        start = data.find('(')
        end = data.find(')') 
        if start == -1:
            record.add_ordered_field(Field(tag='100', indicators=['1',' '], subfields=['a', data, 
                                                                                '4', 'aut']))
            return (data, '')
        author = data[:start]
        code = data[start+1: end]
        record.add_ordered_field(Field(tag='100', indicators=['1',' '], subfields=['a', author, 
                                                                                '4', 'aut',
                                                                                '7', code]))
        return (author, code)
    else:
        return (None, None)

def get_title_subtitle(data):
    split = data.find(':')
    if split == -1:
        return (data, '' )
    else:
        return(data[:split], data[split+1:-1])   

def add_264(row, record):
    if pd.isnull(row['Město vydání, země vydání, nakladatel']):
        return record    
    city_country_publisher = row['Město vydání, země vydání, nakladatel']
    while True:
        city =  re.search('^[\w\s]+', city_country_publisher).group(0)
        if '§' in city_country_publisher:
            start = city_country_publisher.find('§') 
            publisher = re.search('(?<=\:\s).+', city_country_publisher[:start]).group(0)
            year = row['Rok'] 
            print(publisher)
            record.add_ordered_field(Field(tag = '264', indicators = [' ', '1'], subfields = ['a', city + ':', 
                                                                            'b', publisher, 
                                                                            'c', str(int(year))]))
            city_country_publisher = city_country_publisher[start + 1: ]
           
        else:
            break    
    publisher = re.search('(?<=\:\s)[\w\s]+', city_country_publisher).group(0)
    year = row['Rok']
    record.add_ordered_field(Field(tag = '264', indicators = [' ', '1'], subfields = ['a', city + ':', 
                                                                            'b', publisher + ',',
                                                                            'c', str(int(year)) ]))    

def add_translator(translators, record):
    while '§' in translators:
            start = translators.find('§')   
            record.add_ordered_field(Field(tag='700', indicators=['1',' '], subfields=['a', translators[:start ],
                                                                                        '4', 'trl'])) 
            translators = translators[start+1:]
    record.add_ordered_field(Field(tag='700', indicators=['1',' '], subfields=['a', translators,
                                                                                        '4', 'trl']))

def add_commmon(row, record, author):
    record.add_ordered_field(Field(tag='001', indicators = [' ', ' '], data=str('it22'+ "".join(['0' for a in range(6-len(str(row['Číslo záznamu'])))]) + str(row['Číslo záznamu'])))) 
    record.add_ordered_field(Field(tag='003', indicators = [' ', ' '], data='CZ PrUCL')) # institution
    data = create_008(row)
    record.add_ordered_field(Field(tag='008', indicators = [' ', ' '], data = data))
    # ISBN
    if not(pd.isnull(row['ISBN'])):
        record.add_ordered_field(Field(tag='020', indicators=[' ',' '], subfields=['a', str(row['ISBN'])] )) 

    record.add_ordered_field(Field(tag='040', indicators=[' ',' '], subfields=['a', 'ABB060',
                                                                               'c', 'cze',
                                                                               'e', 'rda']))
    record.add_ordered_field(Field(tag='041', indicators=['1',' '],subfields=['a', str(row['Jazyk díla']),
                                                                             'h', str(row['Výchozí jazyk '])]))

    if not(pd.isnull(row['Původní název'])):                                                                          
        record.add_ordered_field(Field(tag='240', indicators = ['1', '0'], subfields = ['a', row['Původní název'], 
                                                                              '1', 'italsky' ]))
    else: 
        record.add_ordered_field(Field(tag='240', indicators = ['1', '0'], subfields = [ '1', 'italsky' ]))    

    (title, subtitle) = get_title_subtitle(row['Název díla dle titulu (v příslušném písmu)'])
    liabiliy = row['Údaje o odpovědnosti a další informace']
    if subtitle == '' and pd.isnull(liabiliy):                                                                          
        record.add_ordered_field(Field(tag = '245', indicators = ['1', '0'], subfields = ['a', title]))                                                                          
    else:
        if pd.isnull(liabiliy):
            record.add_ordered_field(Field(tag = '245', indicators = ['1', '0'], subfields = ['a', title, 
                                                                                    'b', subtitle]))
        elif subtitle == '':     
            record.add_ordered_field(Field(tag = '245', indicators = ['1', '0'], subfields = ['a', title, 
                                                                                    'c', liabiliy]))
        else:
            record.add_ordered_field(Field(tag = '245', indicators = ['1', '0'], subfields = ['a', title,
                                                                                    'b', subtitle + " / ", 
                                   
                                                                                    'c', liabiliy]))
    add_595(record, row, author)  

    if not(pd.isnull(row['Překladatel/ka'])):
        add_translator(row['Překladatel/ka'], record ) 
        
    record.add_ordered_field(Field(tag = '910', indicators=[' ', ' '], subfields=['a', 'ABB060' ] ) )
    record.add_ordered_field(Field(tag='964', indicators = [' ', ' '], data = 'it22'))
    record.add_ordered_field(Field(tag = '964', indicators=[' ', ' '], subfields=['a', 'TRL' ] ) )

    record.add_ordered_field(Field(tag = 'OWN', indicators = [' ', ' '], subfields = ['a', 'UCLA']))

def create_record_part_of_book(row, index, df):
    record = Record(to_unicode=True,
        force_utf8=True)
    print(row['Číslo záznamu'])
    record.leader = '-----naa----------------'  
    ind = row['Je součást čeho (číslo záznamu)']
    book_row = df.iloc[ind]
    tup = add_author_code(book_row['Autor/ka + kód autority'], record)
    author = tup[0]
    add_commmon(row, record, author)  


def create_record_book(row):
    record = Record(to_unicode=True,
        force_utf8=True)

    print(row['Číslo záznamu'])
    record.leader = '-----nam----------------'
    tup = add_author_code(row['Autor/ka + kód autority'], record)
    author = tup[0]
    add_commmon(row, record, author)      
    add_264(row, record)
    if not(pd.isnull(row['Počet stran'])):
        record.add_ordered_field(Field(tag = '300', indicators=[' ', ' '], subfields=['a', str(int(row['Počet stran'])) ]))                                                                             
                                                                             
    if not(pd.isnull(row['tecnická poznámka'])):
        record.add_ordered_field(Field(tag='506', indicators = [' ', ' '], data = row['tecnická poznámka']))

    if not(pd.isnull(row['Zdroj či odkaz'])):
          record.add_ordered_field(Field(tag = '998', indicators=[' ', ' '], subfields=['a', row['Zdroj či odkaz'] ] ) )
    
    return record



with open(OUT , 'wb') as writer:
    for index, row in df.iterrows():
        if 'kniha' in row['Typ záznamu']: 
            record = create_record_book(row)                                                                                                                          
            #print(record)
            #data.write(record.as_dict())
            #data.write(record)
            #data.write(record.as_marc())
            #data.write(record.as_marc21())
            #data.write(record)
        if 'část knihy' in row['Typ záznamu']: 
            record = create_record_book(row) 
        print(record)    
        writer.write(record.as_marc())


writer.close()

