def find(path)
if os.path.exists(path):
    return true
else
subdirs = path.split(os.sep)
if len(subdirs)==0:
    print()
else
    partial_path=""
    for single_subdir in subdirs:
        oldpath = partial_path
        partial_path.append(os.sep).append(single_subdir)
        if os.path.exists(partial_path):
            continue
        else
            os.listdir(oldpath)
            break
