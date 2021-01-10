import os, json 
main_dir = "/home/page_cache_exp/out"
fl = os.walk(main_dir).__next__()[2]

for fn in fl:
    file_path = os.path.join(main_dir, fn)
    data = {}
    with open(file_path) as f:
        data = json.load(f)

    print(data)
    