# Импорт библиотек


import matplotlib.pyplot as plt
import datetime

# Не получилось отловить баг в файле, поэтому нужно менять это число. Оно отвечает за индекс интересующей рыбы
indexes_fish = 102

from upload_and_preprocessing import import_dfs

df_catch, df_product, df_fish, _, _, _, _, _, df_Ext, df_Ext2 = import_dfs()

new_df = df_Ext.merge(df_Ext2, on=["id_vsd"])

for iterator in range(len(df_Ext2)):
        if new_df.iloc[iterator, -1] == "кг":
            new_df.iloc[iterator, -2] = new_df.iloc[iterator, -2]/1000
            
        elif new_df.iloc[iterator, -1] == "тонна":
            continue
        else:
            new_df.iloc[iterator, -2] = new_df.iloc[iterator, -2]/1000
    
def ship_fishing_vessels(id_fish):
    
    print("--"*20)
    my_file.write("\n" +"--"*20)
    list_ships = df_catch[df_catch["id_fish"] == id_fish]["id_ves"].unique()
    print("Список судов, которые официально занимались выловом данной рыбы:\n", list_ships)
    my_file.write("\nСписок судов, которые официально занимались выловом данной рыбы:\n")
    my_file.write(str(list_ships))
    
    return list_ships

def plat_fishing_vessels(name_fish):
    
    print("--"*20)
    my_file.write("\n" +"--"*20)
    list_Plat = new_df[(new_df["id_fish"] == name_fish) & (new_df["id_ves"] == -1)]["id_Plat"].unique()
    print("Список заводов, которые занимались данной рыбой:\n", list_Plat)
    my_file.write("\nСписок заводов, которые занимались данной рыбой:\n")
    my_file.write(str(list_Plat))
    
    return list_Plat

def info_ship(ship, fish, name_fish):
    
    print("--"*20)
    my_file.write("\n" +"--"*20)
    own = df_catch[df_catch["id_ves"] == ship]["id_own"].unique()[0]
    print("Информация по судну номер {} собственника(ов) {}".format(ship, own))
    my_file.write("\nИнформация по судну номер {} собственника(ов) {}".format(ship, own))
    print("Вылов рыбы под номером {}, ({})".format(fish, name_fish))
    my_file.write("\nВылов рыбы под номером {}, ({})".format(fish, name_fish))
        
    
    summa_DB_1 = 0
    summa_DB_2 = 0
    for iterator in range(len(df_catch[(df_catch["id_ves"] == ship) & (df_catch["id_fish"] == fish)])):
        date_isp_DB_1 = df_catch[(df_catch["id_ves"] == ship) & (df_catch["id_fish"] == fish)]["date"].iloc[iterator]
        summa_DB_1 += df_catch[(df_catch["id_ves"] == ship) & (df_catch["date"] == date_isp_DB_1) & (df_catch["id_fish"] == fish)].iloc[0,4]
    summa_DB_1 = round(summa_DB_1, 3)
    print("По официальной информации из первой БД, судном {} было выловлено {} т рыбы под номером {}".format(ship,
                                                                                                             summa_DB_1,
                                                                                                             fish))
    
    my_file.write("\nПо официальной информации из первой БД, судном {} было выловлено {} т рыбы под номером {}".format(ship,
                                                                                                             summa_DB_1,
                                                                                                             fish))
    
    for iterator in range(len(new_df[(new_df["id_ves"] == ship)  & (new_df["id_fish"] == fish)])):
        date_isp_DB_2 = new_df[(new_df["id_ves"] == ship) & (new_df["id_fish"] == fish)]["date_fishery"].iloc[iterator]
        len_sr = len(new_df[(new_df["id_ves"] == ship) & (new_df["date_fishery"] == date_isp_DB_2) & (new_df["id_fish"] == fish)]["volume"])
        if len_sr > 1:
            summa_DB_2 += max(new_df[(new_df["id_ves"] == ship) & (new_df["date_fishery"] == date_isp_DB_2) & (new_df["id_fish"] == fish)]["volume"])/len_sr
        elif len_sr == 1:
            summa_DB_2 += new_df[(new_df["id_ves"] == ship) & (new_df["date_fishery"] == date_isp_DB_2) & (new_df["id_fish"] == fish)].iloc[0, -2]
    summa_DB_2 = round(summa_DB_2, 3)
    print("По информации из второй БД, судном {} за этот же период было выловлено {} т рыбы под номером {}".format(ship,
                                                                                                                    summa_DB_2,
                                                                                                                    fish))
    
    my_file.write("\nПо информации из второй БД, судном {} за этот же период было выловлено {} т рыбы под номером {}".format(ship,
                                                                                                                    summa_DB_2,
                                                                                                                    fish))
    
    if summa_DB_1 == summa_DB_2:
        print("По рыбе номер {} кол-во вылова по первой и второй БД совпадают и равны {} т".format(fish, summa_DB_1))
        my_file.write("\nПо рыбе номер {} кол-во вылова по первой и второй БД совпадают и равны {} т".format(fish, summa_DB_1))
        return 0
    elif summa_DB_1 > summa_DB_2:
        print("По рыбе номер {} кол-во вылова по первой и второй БД не совпадают. По первой БД вылов равен {} т, а по второй {} т".format(fish, summa_DB_1, summa_DB_2))
        my_file.write("\nПо рыбе номер {} кол-во вылова по первой и второй БД не совпадают. По первой БД вылов равен {} т, а по второй {} т".format(fish, summa_DB_1, summa_DB_2))
        print("*** Обнаружена аномаля в вылове рыбы! ***")
        my_file.write("\n*** Обнаружена аномаля в вылове рыбы! ***")
        return 1
    elif summa_DB_1 < summa_DB_2:
        print("По рыбе номер {} кол-во вылова по первой и второй БД не совпадают. По первой БД вылов равен {} т, а по второй {} т".format(fish, summa_DB_1, summa_DB_2))
        my_file.write("\nПо рыбе номер {} кол-во вылова по первой и второй БД не совпадают. По первой БД вылов равен {} т, а по второй {} т".format(fish, summa_DB_1, summa_DB_2))
        print("*** Обнаружена аномаля в вылове рыбы! ***")
        my_file.write("\n*** Обнаружена аномаля в вылове рыбы! ***")
        return 1
    
def info_Plat(plat, name_fish):
    print("--"*20)
    my_file.write("\n" +"--"*20)
    print("Информация по заводу номер {}".format(plat))
    my_file.write("\nИнформация по заводу номер {}".format(plat))
    print("Принята рыба {}".format(name_fish))
    my_file.write("\nПринята рыба {}".format(name_fish))
    sum_plat = sum(new_df[(new_df["fish"] == name_fish) & (new_df["id_ves"] == -1) & (new_df["id_Plat"] == plat)]["volume"])
    print("Заводом {}, принято {} т рыбы {}".format(plat, sum_plat, name_fish))
    my_file.write("\nЗаводом {}, принято {} т рыбы {}".format(plat, sum_plat, name_fish))
    
    return sum_plat

def print_info(id_fish):
    
    name_fish_in_function = df_fish[df_fish["id_fish"] == id_fish]["fish"].iloc[0]
    
    summ_oficial = sum(df_catch[df_catch["id_fish"] == id_fish]["catch_volume"])
    summ_oficial = round(summ_oficial, 3)
    
    summ_ship = sum(new_df[(new_df["fish"] == name_fish_in_function) & (new_df["id_ves"] != -1)]["volume"])
    summ_ship = round(summ_ship, 3)
    
    summ_plat = sum(new_df[(new_df["fish"] == name_fish_in_function) & (new_df["Name_Plat"] != -1) & (new_df["Name_Plat"] != "\\N")]["volume"])
    summ_plat = round(summ_plat, 3)
    
    print("--"*20)
    my_file.write("\n" +"--"*20)
    print("Официально, сумма выловленой рыбы {} за всё время составляет {} т".format(name_fish_in_function, summ_oficial))
    my_file.write("\nОфициально, сумма выловленой рыбы {} за всё время составляет {} т".format(name_fish_in_function, summ_oficial))
    print("Судя по отчетам, сумма выловленой рыбы {} за всё время составляет {} т".format(name_fish_in_function, summ_ship))
    my_file.write("\nСудя по отчетам, сумма выловленой рыбы {} за всё время составляет {} т".format(name_fish_in_function, summ_ship))
    print("Судя по отчетам, сумма полученной рыбы {} за всё время заводами составляет {} т".format(name_fish_in_function, summ_plat))
    my_file.write("\nСудя по отчетам, сумма полученной рыбы {} за всё время заводами составляет {} т".format(name_fish_in_function, summ_plat))
    return name_fish_in_function, summ_plat, summ_oficial, summ_ship


current_date = datetime.date.today()

current_date_time = datetime.datetime.now()
current_date_time = current_date_time.time()
current_date_time = str(current_date_time)[0:-7]
current_date_time = current_date_time.replace(":","-")

my_file = open("file_info_fish_number_" + str(indexes_fish) + "_" + str(current_date) + "_" + str(current_date_time) + ".txt", "w")

name_fish, summ_all_plat, summ_oficial, summ_ship = print_info(indexes_fish)

list_sfv = ship_fishing_vessels(indexes_fish)
list_pfv = plat_fishing_vessels(indexes_fish)

list_anomalies = []

for iterator_list_sfv in list_sfv:
    
    list_anomalies.append(info_ship(iterator_list_sfv, indexes_fish, name_fish))
    
    
summ_plat = 0
    
for iterator_list_pfv in list_pfv:
    
    sum_one_plat = info_Plat(iterator_list_pfv, name_fish)
    summ_plat += sum_one_plat
    
print("--"*20)
my_file.write("\n" +"--"*20)
if summ_oficial == summ_ship:
    print("Аномалии в вылове рыбы отсутсвуют")
    my_file.write("\nАномалии в вылове рыбы отсутсвуют")
    list_anomalies.append(0)
else:
    print("Обнаружена Аномалия в вылове рыбы!")
    my_file.write("\nОбнаружена Аномалия в вылове рыбы!")
    list_anomalies.append(1)
    
print("--"*20)
my_file.write("\n" +"--"*20)
if summ_all_plat == summ_plat:
    print("Аномалии в сбыте рыбы отсутсвуют")
    my_file.write("\nАномалии в сбыте рыбы отсутсвуют")
    list_anomalies.append(0)
else:
    print("Обнаружена Аномалия в сбыте рыбы!")
    my_file.write("\nОбнаружена Аномалия в сбыте рыбы!")
    list_anomalies.append(1)
    
element_count_0 = len([item for item in list_anomalies if item != 1])
element_count_1 = len([item for item in list_anomalies if item != 0])

print("--"*20)
my_file.write("\n" +"--"*20)
print("Кол-во найденных Аномалий: {}".format(element_count_1))
my_file.write("\nКол-во найденных Аномалий: {}".format(element_count_1))

labels = ['Аномалия','Аномалии отсутствуют']
values = [element_count_1, element_count_0]
explode = (0.2, 0)
colors = ['red','green']
plt.title('Кол-во Аномалий',fontsize=20)
plt.pie(values,colors=colors,explode=explode)
plt.axis('equal')
plt.legend(labels=labels, loc='upper center', bbox_to_anchor=(0.5, -0.04), ncol=2,fontsize=15)
plt.savefig("pie_chart_" + str(indexes_fish) + "_" + str(current_date) + "_" + str(current_date_time) + ".png")
plt.show()


my_file.close()

