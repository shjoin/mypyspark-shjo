import os
import sys
import subprocess as sp

avg_dict = {"sum": 0, "count": 0}

def lengthOfLongestSubstring (s:str) -> int:   #cnt = lengthOfLongestSubstring('abcabcbb')
    letter_set=set({})
    letter_list=[]
    a=0
    cnt=0
    for i,x in enumerate(s):
        letter_list.insert(i,x)

    print(f"string become a Lsit {letter_list} ")

    letter_set=set(letter_list)
    cnt=len(letter_set)
    print(f"Count is {cnt}")


    return cnt

def stripnumeric (s:str) -> str:
    ch=""
    for x in s:
       if not x.isnumeric():
           ch= ch +x


    return ch

def uniquenumber (l:list) -> list:#out_list=uniquenumber([1, 1, 1, 2, 3, 4, 4, 1])
    my_set=set(l)
    my_list= list(my_set)

    return my_list

def failed_clusters ():
    faild_cnt =0
    i=1
    server_alllist=[]
    clusters_dict={"A": [1, 2, 3],"B": [3, 4, 5],"C": [2, 4, 6]}
    servers_dict= { 1: 1, 2: 1,3: 1,4: 0, 5: 0, 6: 1,}

    for cluster in clusters_dict:
        server_mylist =clusters_dict.get(cluster)        #for A  [1, 2, 3]
        #print(server_mylist)
        if i == 1:
            i += 1
            server_alllist=server_mylist
        server_alllist.extend(server_mylist)

    my_set=set(server_alllist)
    my_list_server=list(my_set)
    print(my_list_server)

    for server in my_list_server: #unique servers #[1, 2, 3]
        ##print("server no " ,server)
        state=servers_dict.get(server)
        ##print("state",state)
        if state == 0:
           faild_cnt += 1
           clustername= cluster

    print(faild_cnt)

def failed_clusters ():
    #"Write code to return the number of clusters that have at least 1 server in a failed state. Your code should accept a list of
    # clusters as input."

    faild_cnt =0
    i=1
    server_alllist=[]
    clusters_dict={"A": [1, 2, 3],"B": [3, 4, 5],"C": [2, 4, 6]}
    servers_dict= { 1: 1, 2: 1,3: 1,4: 0, 5: 0, 6: 1,}

    for cluster in clusters_dict:
        server_mylist =clusters_dict.get(cluster)        #for A  [1, 2, 3]
        #print(server_mylist)
        if i == 1:
            i += 1
            server_alllist=server_mylist
        server_alllist.extend(server_mylist)

    my_set=set(server_alllist)
    my_list_server=list(my_set)
    print(my_list_server)

    for server in my_list_server: #unique servers #[1, 2, 3]
        ##print("server no " ,server)
        state=servers_dict.get(server)
        ##print("state",state)
        if state == 0:
           faild_cnt += 1
           clustername= cluster #Need to work to check cluster wise failure lets say we have 2 servers in same cluster the ans should be 1

    print(faild_cnt)


def store(event):
    event_dict ={ {"session_id": 1, "start_time": 1626822000, "stop_time": 1626823200},
                  {"session_id": 1, "start_time": 1626823500, "stop_time": 1626823800},
                  {"session_id": 2, "start_time": 1626822600, "stop_time": 1626825600},
                  {"session_id": 3, "start_time": 1626823800, "stop_time": 1626824700}}

    start_time = event["start_time"]
    stop_time = event["stop_time"]

    view_time = stop_time - start_time

    avg_dict["sum"] += view_time
    avg_dict["count"] += 1

def test_membership_operators():
    x=5
    y='hi'
    print(x ,y)
    print(x==y)#False
    print(type(x) is type(y))#False
    print(x == y)#False
    y=10
    print(x == y)#False
    print(type(y),type(x))#<class 'int'> <class 'int'>
    print(type(x) is type(y))  # True
    print(type(x) is not type(y))  # False
    db_user=['db_admin','db_config','db_installation']
    random_user ='db_admin'
    if random_user in db_user:
        print("yes, this is valid user")
    else:
        print('not a valid user')

    print(random_user in db_user)#True
    print(random_user not in db_user)#False
    print(all([2<3,4<6,6<9]))#all==and
    print(all([2 < 3, 4 < 3, 6 < 9]))  # all==and#False
    print(any([2 < 3, 4 < 3, 6 < 9]))  # any==or #true
    print(not any([2 < 3, 4 < 3, 6 < 9]))  # any==or #False

def OS_module():
    path_loc='C:\\Users\\shail\\Documents\\DMS'
   # print(os.listdir(path_loc))#['AWS certificate cloud practitioner.docx', 'AWS CLI_Boto3.docx', 'AWS Database Migration Service.pptx', 'aws redshift', 'AWS TEST_1.txt', 'AWS TEST_1_mod.txt', 'aws-developer_accessKeys.csv', 'AWS.docx', 'AWSCLIV2.msi', 'DataLake Details.docx', 'DataLake GIT', 'DMS CDC.txt', 'dms-api.pdf', 'dms-sbs.pdf', 'dms-ug (1).pdf', 'dms-ug.pdf', 'Doc2.docx', 'Get Started Now with Oracle Cloud.pdf', 'gvim_8.2.2825_x86_signed.exe', 'list_iam_users.py', 'list_s3_buckets.py', 'OneDrive_2021-09-21.zip', 'Oraclecloud connected.sql', 'rootkey.csv', 'rootkey_2.csv', 'TEST DATA.SQL', 'xvxvxcvcx.sql', '~$AWS.docx']
    if os.path.exists(path_loc):
       dirfile=os.listdir(path_loc)
    else:
        print(f"given {path_loc} is invalid")
        sys.exit()

    for each_item in os.listdir(path_loc):
        completepath_loc =os.path.join(path_loc,each_item)
        if os.path.isfile(completepath_loc):
            print(f"{completepath_loc} is a file ")
        else:
            print(f"{completepath_loc} is a dir")

def my_subprocess():
     #var_out=os.system("dir") #not storing this command ,it stores only the command success or not (0 for success ,1 for failed)
     #print(var_out) #0
     cmd='dir'
     #subp=sp.Popen(cmd,shell=True,stdout=sp.PIPE,stderr=sp.PIPE,universal_newlines=True)
     #if shell = True then cmd is a string (as ur os command)
     #if shell = Flase then your cmd is a list

     #note Shell = False Does nt work on ur environment
     #if shell = True ,CMD="ls -ltr" (True is the best for windows )
     #if shell = Flase , cmd ="ls -ltr".split() or ['ls','-ltr']
     str_line = 'Hi Shailendra Please confirm you Python GUI bash, version 3.9.7.8(2)-release2 and let me know asap'
     #print(str_line.split())
     str_list=str_line.split()
     #print(str_line.find('version'))#50  str.find(sub, start, end)
     #print(str_list.index('version'))
     inx=str_list.index('version')
     #print(str_list[inx+1].split('(')[0]) #3.9.7.8


def main():
  #cnt = lengthOfLongestSubstring('abcabcbb')
  #str = stripnumeric('ab2c4ab3c10bb')
  my_list= []
  #out_list=uniquenumber([1, 1, 1, 2, 3, 4, 4, 1])
  #failed_clusters()
  #print(out_list)
  #test_membership_operators()
  #OS_module()
  #my_subprocess()
  str="hello"
  lst=[]
  #print(str.split())
  lst2=str.split()
  print(lst2)
  lst=lst.extend(lst2)
  print(lst)

if __name__ == '__main__':
   print('main Starts ......')
   main()
   print("****DONE MAIN****")