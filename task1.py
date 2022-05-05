import pandas as pd
from sqlalchemy import create_engine

def extract(fileType,fileName,filePath):
    temp=filePath+'/'+fileName
    if fileType==1:
        file=pd.read_csv(temp)
    elif fileType==2:
        file=pd.read_json(temp,lines=True)
    else:
        database='sqlite:///'+fileName+'.db'
        sqlEng = create_engine(database).connect()
        file = pd.read_sql_table(filePath, sqlEng)
    #print('Contents of the File : \n',file)
    transform(file,fileType)
    
def transform(file,fileType):
    ch='y'
    while(ch=='y'):
        column=input('\nEnter the Column Name to Apply Transformation : ')
        for i in range (len(file[column])):
            file[column][i]=file[column][i].capitalize()
        #print(file)
        ch=input('Want to Transoform more Columns ?(y/n) : ')
    writer(file,fileType)
    
def writer(file,fileType):
    if fileType==1 or fileType==2:
        fileName=input('\nEnter the Filename to store the Output : ')
        filePath=input('Enter the File Path where the Output is to be stored : ')
        temp=filePath+'/'+fileName
        if fileType==1:
            file.to_csv(temp)
        elif fileType==2:
            file.to_json(temp)
    else:
        fileName=input('\nEnter the Database Name to store the Output : ')
        dbPath=input('Enter the Database Path where the Output is to be stored : ')
        fileName=dbPath+'/'+fileName
        filePath=input('Enter the Table Name : ')
        database='sqlite:///'+fileName+'.db'
        sqlEng = create_engine(database).connect()
        file.to_sql(filePath, sqlEng)
    
print('Available File Types :\n1. CSV\n2. JSON\n3. SQL')
while(True):
    fileType=int(input('Enter the File Type : '))
    if fileType==1 or fileType==2:
        fileName=input('Enter the Filename : ')
        filePath=input('Enter the File Path : ')
        break
    elif fileType==3:
        fileName=input('Enter the Database Name : ')
        dbPath=input('Enter the Database Path : ')
        fileName=dbPath+'/'+fileName
        filePath=input('Enter the Table Name : ')
        break
extract(fileType,fileName,filePath)
