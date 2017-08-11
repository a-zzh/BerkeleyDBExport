


if __name__ == "__main__":
    with open('D:/test/xxx.txt', 'r') as f:
        line = f.read(1024)
        while line:
            print line
            line =  f.read(1024)
