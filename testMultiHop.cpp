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
#define MEM_SIZE 1000000000
char* to_recv_block_mem = NULL;
char* sendBuf = NULL;
size_t sendLen = 10;

char local_ip = "12.12.10.17";
int local_port = 9999;

char* remote_ip = "12.12.10.16";
int remote_port = 9999;

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
	ret = cro.client_connect_to_server();
	if (ret)
	{
		rdma_error("Failed to setup client connection , ret = %d \n", ret);
		return ret;
	}

	ret = cro.client_send_metadata_to_server1(sendBuf, sendLen);
	if (ret)
	{
		rdma_error("Failed to setup client connection , ret = %d \n", ret);
		return ret;
	}

	while (1 == 1)
	{
		ret = cro.start_remote_write(MEM_SIZE, 0);
	}

}
void rdma_recvTd(int recv_thread_id)
{
	server_rdma_op sro;
	int ret = sro.rdma_server_init(local_ip, local_port, to_recv_block_mem, MEM_SIZE);
	while (1 == 1)
	{


	}

}


int main(int argc, const char * argv[])
{
	printf("Hello\n");

}