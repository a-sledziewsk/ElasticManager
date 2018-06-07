import os
import pandas as pd
from datetime import datetime

'''Program reads the starting and ending date from txt files
in current directory. It assumes structure of the file resembling
wheeljack reports. Remeber that date which you insert in
ReportMaker has to be decreased by 1 hour'''


print('--------------')
cwd = os.getcwd()
print('Your current directory is {}'.format(cwd))
print()

what_inside = os.listdir(cwd)
what_inside = list(filter(lambda x: '.txt' in x, what_inside))

print('REMEMBER THAT DATE PRESENTING BELOW IS IN UTC+1\n\n')
print('Reportmaker formats dates in UTC+0\n\n')

for file in what_inside:
    if os.stat(file).st_size == 0:
        continue
    df = pd.read_csv(file, sep=';')
    df = df['unix_time']
    df.dropna(axis=0, how='all', inplace=True)
    if df.empty:
        print('{} is empty\n'.format(file))
        continue

    first = datetime.fromtimestamp(int(str(df.iloc[0])[:-3]))
    last = datetime.fromtimestamp(int(str(df.iloc[-1])[:-3]))
    print('File {0} includes following period: {1} - {2}\n'.
          format(file, first, last))
