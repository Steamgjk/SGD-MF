#define MAX_CONCURRENCY 20

struct rdma_cm_id* MyRing::RDMA_InitConnection(char* local_ip, char* remote_ip, int remote_port) //as client
{
	struct rdma_cm_id* rc_id = rdma_client_init_connection(local_ip, remote_ip, remote_port);
	return rc_id;
}

struct rdma_cm_id* RDMA_Wait4Connection(int listen_port) //as server
{
	struct rdma_event_channel* rec =  rdma_server_init(listen_port);
	if (rec != NULL)
	{
		struct rdma_cm_id* rc_id = server_wait4conn(rec);
		return rc_id;

	}
	else
	{
		printf("Server Init Fail\n");
		exit(1);
	}
	return NULL;
}


void* concurrency_recv_by_RDMA(struct ibv_wc *wc, uint32_t& recv_len)
{
	struct rdma_cm_id *id = (struct rdma_cm_id *)(uintptr_t)wc->wr_id;
	struct context *ctx = (struct context *)id->context;
	void* _data = nullptr;

	if (wc->opcode == IBV_WC_RECV_RDMA_WITH_IMM)
	{
		uint32_t index = wc->imm_data;
		uint32_t size = *((uint32_t*)(ctx->buffer[index]));
		char* recv_data_ptr = ctx->buffer[index] + sizeof(uint32_t);

		recv_len = size;
		_data = (void*)std::malloc(sizeof(char) * size);

		if (_data == nullptr)
		{
			printf("fatal error in recv data malloc!!!!\n");
			exit(-1);
		}
		std::memcpy(_data, recv_data_ptr, size);

		_post_receive(id, wc->imm_data);
		_ack_remote(id, wc->imm_data);

	}
	else if (wc->opcode == IBV_WC_RECV)
	{
		switch (ctx->k_exch[1]->id)
		{
		case MSG_MR:
		{
			log_info("recv MD5 is %llu\n", ctx->k_exch[1]->md5);
			log_info("imm_data is %d\n", wc->imm_data);
			for (int index = 0; index < MAX_CONCURRENCY; index++)
			{
				ctx->peer_addr[index] = ctx->k_exch[1]->key_info[index].addr;
				ctx->peer_rkey[index] = ctx->k_exch[1]->key_info[index].rkey;
				struct sockaddr_in* client_addr = (struct sockaddr_in *)rdma_get_peer_addr(id);

			}
		} break;
		default:
			break;
		}
	}
	return _data;
}
void RDMA_ProcessRecvData(struct rdma_cm_id* rc_id)
{

	struct ibv_cq *cq = NULL;
	//struct ibv_wc wc;
	struct ibv_wc wc[MAX_CONCURRENCY * 2];
	struct context *ctx = (struct context *)rc_id->context;
	void *ev_ctx = NULL;
	while (!shut_down)
	{
		TEST_NZ(ibv_get_cq_event(ctx->comp_channel, &cq, &ev_ctx));
		ibv_ack_cq_events(cq, 1);
		TEST_NZ(ibv_req_notify_cq(cq, 0));

		while (true)
		{
			int wc_num = ibv_poll_cq(cq, MAX_CONCURRENCY * 2, wc);
			if (shut_down)
			{
				break;
			}

			for (int index = 0; index < wc_num; index++)
			{
				if (wc[index].status == IBV_WC_SUCCESS)
				{
					/*****here to modified recv* wc---->wc[index]****/
					void* recv_data = nullptr;
					uint32_t recv_len;

					recv_data = concurrency_recv_by_RDMA(&wc[index], recv_len);
					if (recv_data != nullptr)
					{

					}


				}
				else
				{
					printf("\nwc = %s\n", ibv_wc_status_str(wc[index].status));
					rc_die("poll_cq3: status is not IBV_WC_SUCCESS");
				}
			}

		}
	}
}