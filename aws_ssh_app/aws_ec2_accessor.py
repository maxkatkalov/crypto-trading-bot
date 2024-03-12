import asyncio
from dotenv import load_dotenv
from colorama import Fore, Style

from utils.log_formmating import current_timedate_and_func_name

from db_app.config import aws_clients_ec2

load_dotenv()


def get_all_ec2_dns_names():
    active_dns_names = []

    print(
        (
            f"{current_timedate_and_func_name()} "
            f"{Fore.CYAN}Getting active EC2 DNS names...{Style.RESET_ALL}"
        )
    )

    for client in aws_clients_ec2:
        instances = client.describe_instances()

        for reservation in instances["Reservations"]:
            for instance in reservation["Instances"]:
                if instance["State"]["Name"] == "running" and instance["PublicDnsName"] != "ec2-184-72-156-151.compute-1.amazonaws.com":
                    active_dns_names.append(
                        {
                            "dns_name": instance["PublicDnsName"],
                            "instance_id": instance["KeyName"] + ".pem",
                        }
                    )

    print(
        (
            f"{current_timedate_and_func_name()} "
            f"{Fore.GREEN}Got active EC2 DNS names{Style.RESET_ALL}, "
            f"{Fore.BLUE}total: {len(active_dns_names)}{Style.RESET_ALL}."
        )
    )
    print(active_dns_names)
    return active_dns_names


async def start_stop_instances():
    for client in aws_clients_ec2:
        instances = client.describe_instances()

        for reservation in instances["Reservations"]:
            for instance in reservation["Instances"]:
                if instance["State"]["Name"] == "running" and instance["PublicDnsName"] != "ec2-184-72-156-151.compute-1.amazonaws.com":
                    client.stop_instances(InstanceIds=[instance["InstanceId"]])
                    print(f"{current_timedate_and_func_name()} Stopped instance {instance['InstanceId']}, old IP: {instance['PublicIpAddress']}")
    await asyncio.sleep(60)
    for client in aws_clients_ec2:
        instances = client.describe_instances()

        for reservation in instances["Reservations"]:
            for instance in reservation["Instances"]:
                if instance["State"]["Name"] == "stopped":
                    client.start_instances(InstanceIds=[instance["InstanceId"]])
                    print(f"{current_timedate_and_func_name()} Started instance {instance['InstanceId']}.")

    print(f"{current_timedate_and_func_name()} Seeping... 3 minutes left")
    await asyncio.sleep(60)
    print(f"{current_timedate_and_func_name()} Seeping... 2 minutes left")
    await asyncio.sleep(60)
    print(f"{current_timedate_and_func_name()} Seeping... 1 minutes left")
    await asyncio.sleep(60)


if __name__ == "__main__":
    asyncio.run(start_stop_instances())
