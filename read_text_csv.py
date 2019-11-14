from io import StringIO
import csv


csv_header = ['one', 'two', 'three', 'four']

def convert_csv_to_dict(csv_text):
    print(csv_text)

    csv_file = StringIO(csv_text)
    reader = csv.DictReader(csv_file, fieldnames=csv_header)

    data_list = []
    for row in reader:
        if len(row) != len(csv_header):
            return []
            
        data_list.append(dict(row))
        print(row)

    return data_list

if __name__ == "__main__":
    csv_text = "aaa,bbb,ccc,dd\ne,f,g,h\ni,,j,k"
    print(convert_csv_to_dict(csv_text))
