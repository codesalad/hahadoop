def main():
    stream = [3, 1, 4, 1, 5, 9, 2, 6, 5]
    hashStream(2, 1, stream)
    hashStream(3, 7, stream)
    hashStream(4, 0, stream)
    return

def hashStream(a, b, list):
    print('Hashing with: a={:d}, b={:d}'.format(a, b))
    count = 0
    for i in list:
        hashed = hash(a, b, i)
        bitstring = '{:05b}'.format(hashed)
        if bitstring[3:6] == '11': count += 1
        print( '{:d} \t{:s}'.format(hashed, bitstring) )
        
def hash(a, b, x):
    return ((a * x) + b) % 32



if __name__ == "__main__":
    main()
    
