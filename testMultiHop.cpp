//
//  main.cpp
//  linux_socket_api
//
//  Created by Jinkun Geng on 18/5/2.
//  Copyright (c) 2018年 Jinkun Geng. All rights reserved.
//

#include <iostream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <signal.h>
#include <unistd.h>
#include <stdlib.h>
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <cmath>
#include <time.h>
#include <vector>
#include <list>
#include <queue>
#include <map>
#include <thread>
#include <chrono>
#include <algorithm>
#include <mutex>
#include <atomic>
#include <iostream>
#include <fstream>
#include <sys/time.h>
#include <queue>
#include "server_rdma_op.h"
#include "client_rdma_op.h"
using namespace std;
#define CAP 20
#define MEM_SIZE 1000000000
char* to_recv_block_mem = NULL;
char* sendBuf = NULL;
size_t sendLen = 10;

char* ips[CAP] = {"12.12.10.17", "12.12.10.18", "12.12.10.19"};
char local_ip = NULL;
int local_port = 9999;

char* remote_ip = NULL;
int remote_port = 9999;

int rank = -2;
bool forward = false;
void rdma_sendTd(int send_thread_id)
{
	struct sockaddr_in server_sockaddr;
	bzero(&server_sockaddr, sizeof server_sockaddr);
	server_sockaddr.sin_family = AF_INET;
	server_sockaddr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	get_addr(remote_ip, (struct sockaddr*) &server_sockaddr);
	server_sockaddr.sin_port = htons(remote_port);
	client_rdma_op cro;
	int ret = 0;
	ret = cro.client_prepare_connection(&server_sockaddr);
	if (ret)
	{
		rdma_error("Failed to setup client connection , ret = %d \n", ret);
		return ret;
	}
	ret = cro.client_pre_post_recv_buffer();
	if (ret)
	{
		rdma_error("Failed to setup client connection , ret = %d \n", ret);
		return ret;
	}
	printf("to connect ....\n");
	ret = cro.client_connect_to_server();
	if (ret)
	{
		rdma_error("Failed to setup client connection , ret = %d \n", ret);
		return ret;
	}
	printf("to send meta data...\n");
	ret = cro.client_send_metadata_to_server1(sendBuf, sendLen);
	if (ret)
	{
		rdma_error("Failed to setup client connection , ret = %d \n", ret);
		return ret;
	}

	printf("client Init OK\n");
	while (1 == 1)
	{
		printf("start write to remote\n");

		if (forward)
		{
			ret = cro.start_remote_write(MEM_SIZE, 0);
			forward = false;
		}
	}

}
void rdma_recvTd(int recv_thread_id)
{
	server_rdma_op sro;
	int ret = sro.rdma_server_init(local_ip, local_port, to_recv_block_mem, MEM_SIZE);
	printf("server Init OK\n");
	to_recv_block_mem[MEM_SIZE - 1] = '#';
	while (1 == 1)
	{
		//std::this_thread::sleep_for(std::chrono::milliseconds(1000));
		printf("to_recv_block_mem %c\n", to_recv_block_mem[MEM_SIZE - 1] );
		if (to_recv_block_mem[MEM_SIZE - 1] == 'a')
		{
			printf("forward ok\n");
			forward = true;
			to_recv_block_mem[MEM_SIZE - 1] == '#';
		}

	}

}


int main(int argc, const char * argv[])
{
	printf("Hello\n");
	bool isSta = false;
	bool isEnd = false;
	rank = atoi(argv[1]);
	if (rank == 0)
	{
		isSta = true;
		remote_ip = "12.12.10.18";
	}
	else if (rank == -1)
	{
		isEnd = true;
		local_ip = "12.12.10.19";
	}
	else
	{
		local_ip = ips[rank];
		remote_ip = ips[rank + 1];
	}
	to_recv_block_mem = (char*)malloc(MEM_SIZE);
	sendBuf = to_recv_block_mem;
	for (int i = 0; i < MEM_SIZE; i++)
	{
		to_recv_block_mem[i] = 's';
	}
	to_recv_block_mem[MEM_SIZE - 1] = 'a';
	sendLen = MEM_SIZE;
	if (!isSta)
	{
		std::thread recv_thread(rdma_recvTd, 0);
		recv_thread.detach();
	}
	if (!isEnd)
	{
		std::thread send_thread(rdma_sendTd, 0);
		send_thread.detach();
	}


	while (1 == 1)
	{

	}

}