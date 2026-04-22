from Cryptodome.Cipher import AES
from Cryptodome.Protocol.KDF import PBKDF2
import base64 ,os,sys
from Cryptodome.Util.Padding import pad,unpad
from resources.dev import config

try:
    salt=config.salt
    key = config.key
    iv = config.iv

    if not(salt,key,iv):
        raise Exception(F"Error while fetching details for key/iv/salt")
except Exception as e:
    print(f"Error occured. Details : {e}")


def get_privet_key():
    Salt=salt.encode('utf-8')
    kdf=PBKDF2(key,salt,dkLen=64,count=10000)
    return kdf[:32]

def encrypt(raw):
    raw=pad(raw.encode(),AES.block_size)
    cipher = AES.new(get_privet_key(),AES.MODE_CBC, iv.encode('utf-8'))
    return base64.b64encode(cipher.encrypt(raw))

def decrypt(enc):
    cipher= AES.new(get_privet_key(),AES.MODE_CBC,iv.encode('utf-8'))
    return unpad(cipher.decrypt(base64.b64decode(enc)),AES.block_size).decode()

