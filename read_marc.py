from pymarc import MARCReader

my_marc_file = "marc_it.mrc"

#my_marc_file="data/czech_translations_full_18_01_2022.mrc"

with open(my_marc_file, 'rb') as data:
    reader = MARCReader(data, to_unicode=True, force_utf8=True, utf8_handling="strict")

    for record in reader:
        if record is None:
            print(
                "Current chunk: ",
                reader.current_chunk,
                " was ignored because the following exception raised: ",
                reader.current_exception
            )
        else:
            print (record.title())