import re
class Filters:
    list_filter=[]
    v_cnt=1
    def add (self,filter):
        if Filters.v_cnt==1:
           Filters.list_filter=filter.split()
           Filters.v_cnt += 1
           print(Filters.list_filter)
        else:
           Filters.list_filter.extend(filter.split())
        #print('test',Filters.list_filter)


    def isMatch(self,word):# return T/F
        #print(word[0:2])
        srch_str=word[0:2]
        print(srch_str)
        flag=0
        for str in Filters.list_filter:
            #print(str + ' '+ srch_str)
            x= re.findall(srch_str,str)
            #print(x) # Return a list ['he'] or []
            if x:
                flag=1

        if flag==1:
          return "T"
        else:
          return "F"



print("start")
#lst=[]
#lst="shai"
#print(lst.split())
f=Filters()
f.add("he*o")
f.add("he*p")
f.add("a*b")
f.add("fo*")
#print(Filters.list_filter)
print(f.isMatch("hello"))#T
#print(f.isMatch("help"))#T
#print(f.isMatch('foreground'))#T
#print(f.isMatch('f0dfsdf'))#F
#isMatch("help)


'''
sample_str = "Hello World !!"
Like, sample_str[i] will return a character at index i-th index position. Let’s use it.
# Get first character of string i.e. char at index position 0
first_char = sample_str[0]
start_index_pos: Index position, from where it will start fetching the characters, the default value is 0
end_index_pos: Index position till which it will fetch the characters from string, default value is the end of string
step_size: Interval between each character, default value is 1.
To get the first N characters of the string, we need to pass start_index_pos as 0 and end_index_pos  as N i.e.
sample_str[ 0 : N ]

import re

txt = "hello world"

#Search for a sequence that starts with "he", followed by two (any) characters, and an "o":

x = re.findall("he..o", txt)
print(x)  #['hello']

import re

txt = "hello world"

#Check if the string starts with 'hello':

x = re.findall("^hello", txt)
if x:
  print("Yes, the string starts with 'hello'")
else:
  print("No match") #Yes, the string starts with 'hello
mport re

  class Filters:
    def add(self, filter):
   
    def isMatch(self, word): return T/F
       list=word.split
       st = lsi[0]+ list[1]
      filt=Filters.add()
      x=re.finball(st,filt)
 
Filters:
  he*o
  fo*
 
Word:
  hello -> true
  foreground -> true
  help -> false
 
* - matches 0 or more of anything

f = Filters()
f.add("he*o")
f.add("he*p")
f.add("he*q")
f.add("a*b")
f.isMatch("hello")
                       f.isMatch("hello")
'''
