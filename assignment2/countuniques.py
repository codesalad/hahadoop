def main():
    counter = 0
    file = open('shakespeareterms_filter.txt', 'r')
    terms = file.read().split('\n')

    for line in terms:
        if line.split('\t')[1] == '1':
            counter +=1
    file.close()
    print('Unique terms: {:d}'.format(counter))

if __name__ == "__main__":
    main()
    
