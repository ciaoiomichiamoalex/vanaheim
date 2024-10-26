from share.common import decode_json
import paramiko
import os

__version__ = '1.0.0'

PATH_PRJ = 'c:/source/vanaheim'
PATH_CFG = f'{PATH_PRJ}/config/sshmng.json'


def conn_ini(conn_name: str = 'main') -> paramiko.SSHClient:
    """Read from config/sshmng.json the SSH configuration and start the connection.

    :param str conn_name: Refers to SSH configuration name in sshmng.json, defaults to 'main'.
    :return: A reference to the SSH connection.
    """
    config = decode_json(PATH_CFG, 'name', conn_name)
    if not config:
        raise ValueError(f'conn_ini: no config <{conn_name}> found!')

    conn = paramiko.SSHClient()

    if config[0]['host_keys'] and os.path.exists(config[0]['host_keys']):
        conn.load_host_keys(config[0]['host_keys'])
    else:
        conn.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    if config[0]['private_key'] and os.path.exists(config[0]['private_key']):
        conn.connect(
            hostname=config[0]['server'],
            port=config[0]['port'],
            username=config[0]['username'],
            pkey=paramiko.RSAKey.from_private_key_file(config[0]['private_key'])
        )
    else:
        conn.connect(
            hostname=config[0]['server'],
            port=config[0]['port'],
            username=config[0]['username'],
            password=config[0]['password']
        )

    return conn


def sftp_upload(conn: paramiko.SSHClient, local: str, remote: str) -> None:
    """Upload file on SFTP server.

    :param SSHClient conn: The connection achieved from conn_ini() calling.
    :param str local: The local file path.
    :param str remote: The remote file path, including filename.
    """
    sftp = conn.open_sftp()
    sftp.put(local, remote)
    sftp.close()


def sftp_download(conn: paramiko.SSHClient, remote: str, local: str) -> None:
    """Download file from SFTP server.

    :param SSHClient conn: The connection achieved from conn_ini() calling.
    :param remote: The remote file path.
    :param local: The local file path, including filename.
    """
    sftp = conn.open_sftp()
    sftp.get(remote, local)
    sftp.close()
