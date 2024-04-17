#print('shailendr \"Joshi\"')
#print('shailendr \"Joshi\"')
x = 'hi Shailendra Joshi'
y = 'not working today relaxing..'
z="python is very easy"
str="python./i pot porking"

'''
print(dir(x))
print(x.title())
print(y.capitalize()) # only first char will be in caps
print(x.swapcase())
print(x.upper())
print(x.casefold())
print(str)
print(str.strip('not'))
print(str.count('not'))
print(str.index('pot'))
print(str.index('p',11))
print(str.find('z',11))
print(str.index('z',11))
'''

#sal = eval(input("Please enter your Salary: "))
#print(sal*20/100)

my_list=[10,20,30,40,"python",50,60,40,80,90]
print(my_list[0])
print(my_list[0:10])
my_list[0]=100
print(my_list[:])
#print(dir(my_list))
print(my_list.index(40))
print(my_list.index(40,4)) #from 4 position
my_list.append(200)
#my_list.clear()
print(my_list)

my_list.insert(0,10)
print(my_list)
my_new_list =['Hi','Shailendra12']
my_list.append(my_new_list)
print( my_list)
my_list.extend(my_new_list)
print(my_list)
#my_list.pop() #remove last position data
print(my_list.pop()) #remove last position data also shows what was deleted
print(my_list)

#print(my_list.pop(0)) #remove position data also shows what was deleted aslo
#my_list.remove(len(my_list))
#my_list.sort()

'''
my_list.remove('Hi')
my_list.remove('Shailendra')
my_list.remove('python')
'''

print(my_list)
#my_list.sort()
print(my_list)
my_list.reverse()
print(my_list)
#my_list.sort(reverse=True)
#print(my_list)

print('tuple')
my_tuple=(4756191565,475675454,10000022)
print(bool(my_tuple))
my_new_tuple=(4756191565,475675454,['Hi','Shailendra'],10000022)
print(my_new_tuple[2]) #['Hi', 'Shailendra']
print(my_new_tuple[2][1]) #Shailendra
print(my_new_tuple[2][0]) #Hi
#my_tuple[0]=100000000  # immutable Error
print(my_tuple[0])
x=1,2,3
y=19,
print(type(x))
print(type(y))
my_dict={'fruit':'apple','animal':'fox',1:'one','two':2}
print(my_dict[1])
print(my_dict['animal'])
#print(my_dict['three']) #error
print(my_dict.get('three') )  #None


my_dict['three'] =3
print(my_dict)
my_dict['three'] =56
print(my_dict)
print(my_dict.keys())
print(my_dict.values()) #dict_values(['apple', 'fox', 'one', 2, 56])
print(my_dict.items()) #dict_items([('fruit', 'apple'), ('animal', 'fox'), (1, 'one'), ('two', 2), ('three', 56)])


#str= input("Enter a String(word) : ")

'''
str='  Hi Shailendra how are you doing'
print(str.split())
my_lst=str.split()
print(type(my_lst))
#print(my_lst)
#my_lst.reverse()
#print("First " ,my_lst)
# OR
print(my_lst[::-1])
output=" ".join(my_lst[::-1])
print(output)
#print(my_lst[::-1])

my_int_list =[1,2,3,4,5,6,7]
my_int_list.reverse()
print(my_int_list)
'''

#str="ab5cab3cbb"
#s=str.strip("5") only remove from end and from last
#print(s)
'''
my_list=[10,20,30,40,"python",50,60,40,80,90]
my_new_list =['Hi','Shailendra']
my_list.append(my_new_list)
print(my_list)  #[10, 100, 20, 30, 40, 'python', 50, 60, 40, 80, 90, 200, ['Hi', 'Shailendra']]
print(my_list[12,1])
#my_list.extend(my_new_list)
#print(my_list) #[10, 100, 20, 30, 40, 'python', 50, 60, 40, 80, 90, 200, 'Hi', 'Shailendra']
'''
'''
clusters_dict={"A": [1, 2, 3],"B": [3, 4, 5],"C": [2, 4, 6]}
Servers= { 1: 1, 2: 1,3: 1,4: 0, 5: 0, 6: 1,}

#print(dir(clusters_dict))
print(clusters_dict.keys())
print(clusters_dict.get("A"))
mylist =clusters_dict.get("A")
print(Servers.get(4))


event_dict ={ {"session_id": 1, "start_time": 1626822000, "stop_time": 1626823200},
                  {"session_id": 1, "start_time": 1626823500, "stop_time": 1626823800},
                  {"session_id": 2, "start_time": 1626822600, "stop_time": 1626825600},
                  {"session_id": 3, "start_time": 1626823800, "stop_time": 1626824700}}
print(event_dict.keys())


'''

