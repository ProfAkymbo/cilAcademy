import time
def decrypt (encrypttext, key):

    """
    encrpytext: encrypt text that needs to be descipher
    key: Key used to locked to the encrpyt text
    Return: Plaintext encrypt
    
    """
    

    deciphertext = ""

    # add an increament on the key because of python positional indexing
    key +=1
    
    # use string slice index to slice through the string
    character = encrypttext[::key]

    # add the extarcted string to the empy string above
    deciphertext = deciphertext + character

    return deciphertext

# Main program starts here...
#Input...
deciphertext = input("Enter a message to encrypt text: ")
key = int(input("Input a key as a number between 1 and 10: "))
while not (key>=1 and key<=10):
    print("Invalid key, try again!")
    key = int(input("Input a key as a number between 1 and 10"))



# start_time = time.time()
# # print(start_time)
# print("----------------------------------------")
# print(print(f'Encrypt Text:{deciphertext}'))
time.sleep(1)
deciphertext = decrypt(deciphertext, key)
# stop_time = time.time()
# print(stop_time)
# print("------------------------------------------")
# print(f'"Total runtime" = {start_time}-{stop_time}/60' + "min")
#Output...
print("Diphertext:")
print(deciphertext)