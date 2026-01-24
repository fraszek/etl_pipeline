zeby dodac wszystko do db:

    1. run setup.sql in postrges
    2. run etl1.py (zmienione column names w porównaniu do starego) i load.py (approx. 15 min)
    3. run normalize.py (approx. 3 min)
    4. run match.sql in postrges (apporox. 2 min)

to wszystko bedzie jednym skryptem, ale nie dzisiaj
