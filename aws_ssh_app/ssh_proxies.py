import os
import asyncio

import asyncssh
from asyncssh.misc import HostKeyNotVerifiable
from colorama import Fore, Style
from dotenv import load_dotenv


from aws_ssh_app.aws_ec2_accessor import get_all_ec2_dns_names
from utils.log_formmating import current_timedate_and_func_name

load_dotenv()


USERNAME = os.getenv("ssh_server_username")
PORT = int(os.getenv("ssh_server_port"))
KEYS_PATH = os.getenv("general_keys_path")


async def add_server_to_known_hosts(hostname: str) -> None:
    print(f"{Fore.CYAN}Adding {hostname} to known hosts...{Style.RESET_ALL}")

    process = await asyncio.create_subprocess_shell(
        f"ssh-keyscan {hostname} >> ~/.ssh/known_hosts",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )

    stdout, stderr = await process.communicate()

    if process.returncode == 0:
        print(f"{Fore.GREEN}Added {hostname} to known hosts{Style.RESET_ALL}")
    else:
        print(
            f"{Fore.RED}Return code:{Style.RESET_ALL} {process.returncode}",
            f"{Fore.RED}Stdout:{Style.RESET_ALL} {stdout.decode()}",
            f"{Fore.RED}Stderr:{Style.RESET_ALL} {stderr.decode()}"
        )


async def restart_tinyproxy(
        server: dict,
        key_path: str,
        port: int = PORT,
        username: str = USERNAME,
        command: str = "sudo systemctl restart tinyproxy"
):
    print(f"{current_timedate_and_func_name()} Connecting to {Fore.YELLOW}{server}{Style.RESET_ALL}")

    try:
        async with asyncssh.connect(
                host=server,
                port=port,
                username=username,
                client_keys=key_path,
        ) as conn:
            print(f"{current_timedate_and_func_name()} Connected to {Fore.GREEN}{server}{Style.RESET_ALL}")
            await conn.run(command)
            print(f"{current_timedate_and_func_name()} {Fore.RED}{command}{Style.RESET_ALL} succeeded with {server}")
    except HostKeyNotVerifiable as e:
        print(f"{current_timedate_and_func_name()} {Fore.RED}{e}{Style.RESET_ALL}")
        await add_server_to_known_hosts(server)
        await restart_tinyproxy(server, key_path=key_path)
    except Exception as e:
        print(f"{current_timedate_and_func_name()} {Fore.RED}{e}{Style.RESET_ALL}, sleeping for 30 seconds")
        await asyncio.sleep(30)
        await restart_tinyproxy(server, key_path=key_path)


async def edit_tinyproxy_conf(
    conn
):
    edit_config_command = 'echo "Allow 0.0.0.0/0" | sudo tee -a /etc/tinyproxy/tinyproxy.conf'
    restart_tinyproxy_cmd = 'sudo systemctl restart tinyproxy'
    edit_config_res = await conn.run(edit_config_command)
    print(
        f"{current_timedate_and_func_name()} {Fore.RED}{edit_config_command}{Style.RESET_ALL} result: {edit_config_res}")
    restart_tinyproxy_res = await conn.run(restart_tinyproxy_cmd)
    print(
        f"{current_timedate_and_func_name()} {Fore.RED}{restart_tinyproxy_cmd}{Style.RESET_ALL} result: {restart_tinyproxy_res}"
    )


async def make_apt_update(conn):
    upd_command = "sudo apt-get update"
    upd_command_res = await conn.run(upd_command, check=True)

    print(f"{current_timedate_and_func_name()} {Fore.RED}{upd_command}{Style.RESET_ALL} result: {upd_command_res}")


async def install_tiny_proxy(conn):
    tiny_proxy_install = "sudo apt-get -y install tinyproxy"
    tiny_proxy_install_res = await conn.run(tiny_proxy_install, check=True)

    print(
        f"{current_timedate_and_func_name()} {Fore.RED}{tiny_proxy_install}{Style.RESET_ALL} result: {tiny_proxy_install_res}"
    )


async def check_proxy_installetion(
        server: str,
        key_path: str,
        username: str = USERNAME,
):
    print(f"{current_timedate_and_func_name()} Connecting to {Fore.YELLOW}{server}{Style.RESET_ALL}")
    try:
        async with asyncssh.connect(
                host=server,
                username=username,
                client_keys=key_path
        ) as conn:
            check_tiny_proxy = "sudo which tinyproxy"
            res = await conn.run(check_tiny_proxy)
            if not res.stdout:
                print(f"{current_timedate_and_func_name()} Installing tinyproxy on {Fore.GREEN}{server}{Style.RESET_ALL}")
                await make_apt_update(conn)
                await install_tiny_proxy(conn)
                await edit_tinyproxy_conf(conn)
            else:
                print(f"Tinyproxy already installed on {server}, {res.stdout}")

    except HostKeyNotVerifiable as e:
        print(f"{current_timedate_and_func_name()} {Fore.RED}{e}{Style.RESET_ALL}")
        await add_server_to_known_hosts(server)
        await restart_tinyproxy(server, key_path=key_path)


async def connect_all_servers_and_run_cmd(
        hostnames: list[dict[str, str]],
        task: callable = restart_tinyproxy
):
    async with asyncio.TaskGroup() as tg:
        for hostname in hostnames:
            tg.create_task(
                    task(
                        server=hostname.get('dns_name'),
                        key_path=f"{KEYS_PATH}{hostname.get('instance_id')}"
                    ),
            )


if __name__ == '__main__':
    asyncio.run(
        connect_all_servers_and_run_cmd(
            get_all_ec2_dns_names(),
        )
    )
