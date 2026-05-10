from Cryptodome.Util.Padding import pad,unpad
from Cryptodome.Protocol.KDF import PBKDF2
from Cryptodome.Cipher import AES
from resources.dev import config
from src.main.utility import logging_config
import base64,os,sys


try:
    salt=config.salt
    key=config.key
    iv=config.iv

    if not (salt,key,iv):
        raise Exception("salt,key,iv are not found")
except Exception as e:
    print(e)

def get_priver_key():
    Salt=salt.encode('utf-8')
    kdf=PBKDF2(key,Salt,dkLen=64,count=100000)
    return kdf[:32]

def encrypt(raw):
    logging_config.logger.info('encryption started')
    raw_bytes = raw.encode('utf-8')  # step 1
    padded_data = pad(raw_bytes, AES.block_size)  # step 2
    cipher = AES.new(get_priver_key(), AES.MODE_CBC, iv.encode('utf-8'))
    encrypted = cipher.encrypt(padded_data)  # step 3

    return base64.b64encode(encrypted)

def decrypt(enc):
    logging_config.logger.info('decryption started')
    enc=base64.b64decode(enc)
    cipher=AES.new(get_priver_key(),AES.MODE_CBC,iv.encode('utf-8'))
    return unpad(cipher.decrypt(enc),AES.block_size).decode('utf-8')
