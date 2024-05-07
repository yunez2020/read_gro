"""
This program retrieves gro_daily_property files from sftp server
Currently at simplermpbucket
"""


import os
import paramiko

sftpURL   =  'simplermpbucket.blob.core.windows.net'
sftpUser  =  'simplermpbucket.gro-downloads.ola'
sftpPass  =  'UKMZlRs9gthq0LUyUDSKHhLK075AZBdC'
sftpPath  =  '/gro_daily/'
ssh = paramiko.SSHClient()
# automatically add keys without requiring human intervention
ssh.set_missing_host_key_policy( paramiko.AutoAddPolicy() )
ssh.connect(sftpURL, username=sftpUser, password=sftpPass)

sftp = ssh.open_sftp()
local_path='C:/Users/luism/Dropbox/My Documents/SimpleRMP/Ola/read_gro/attachments/'
remote_files = sftp.listdir(sftpPath)
local_files=os.listdir(local_path)


for archivo in remote_files:
    if archivo not in local_files:
        file_remote=sftpPath + archivo
        file_local = local_path + archivo

        print(file_remote + '>>>>' + file_local)
        sftp.get(file_remote, file_local)

sftp.close()
ssh.close()