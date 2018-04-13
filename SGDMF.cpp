/**

评分矩阵R如下

   D1 D2 D3 D4

U1 5  3  -  1

U2 4  -  -  1

U3 1  1  -  5

U4 1  -  -  4

U5 -  1  5  4

***/

#include <iostream>
#include <cstdio>
#include <cstdlib>
#include <cmath>
#include <time.h>
#include <vector>
#include <list>
#include <thread>
#include <chrono>
#include <algorithm>
#include <mutex>
using namespace std;

#define N  8 //用户数
#define M  8 //物品数
#define K  2 //主题个数
#define WORKER_NUM  60
int THREAD_NUM = 20;

double R[N * M];
double P[N * K];
double Q[M * K];
double Qt[K * M];
mutex Pmtx[WORKER_NUM];
mutex Qmtx[WORKER_NUM];
bool worker_debug = false;
bool main_debug = false;
struct Block
{
  int block_id;
  int data_age;
  int sta_idx;
  int psz; //height
  vector<double> eles;
  Block()
  {

  }
  Block operator=(Block& bitem)
  {
    block_id = bitem.block_id;
    data_age = bitem.data_age;
    psz = bitem.psz;
    eles = bitem.eles;
    sta_idx = bitem.sta_idx;
    return *this;
  }
  void printBlock()
  {
    printf("block_id  %d\n", block_id);
    printf("data_age  %d\n", data_age);
    for (int i = 0; i < eles.size(); i++)
    {
      printf("%lf\t", eles[i]);
    }
    printf("\n");
  }
};
Block*Pblocks = NULL;
Block*Qblocks = NULL;
struct Updates
{
  int block_id;
  int clock_t;
  vector<double> eles;
  Updates()
  {

  }
  Updates operator=(Updates& uitem)
  {
    block_id = uitem.block_id;
    clock_t = uitem.clock_t;
    eles = uitem.eles;
    return *this;
  }

  void printUpdates()
  {
    printf("update block_id %d\n", block_id );
    printf("clock_t  %d\n", clock_t);
    printf("ele size %ld\n", eles.size());
    for (int i = 0; i < eles.size(); i++)
    {
      printf("%lf\t", eles[i]);
    }
    printf("\n");
  }
};

vector<Updates> ThreadUpdadtesP[WORKER_NUM];
vector<Updates> ThreadUpdadtesQ[WORKER_NUM];
int head_ptrs_P[WORKER_NUM];
int tail_ptrs_P[WORKER_NUM];
int head_ptrs_Q[WORKER_NUM];
int tail_ptrs_Q[WORKER_NUM];
int cap = 1000;

void worker_thread(int worker_id);

void matrix_factorization(double *minR, double *minP, double *minQ, int minN, int minM, int minK, int steps = 50, float alpha = 0.0002, float beta = 0.02)
{
  double error = 0;
  double* updtP = new double[minN * minK];
  double* updtQ = new double[minM * minK];
  for (int i = 0; i < minN * minK; i++)
  {
    updtP[i] = 0;
  }
  for (int i = 0; i < minM * minK; i++)
  {
    updtQ[i] = 0;
  }
  for (int step = 0; step < steps; ++step)
  {
    for (int i = 0; i < minN; ++i)
    {
      for (int j = 0; j < minM; ++j)
      {
        if (R[i * minM + j] > 0)
        {
          //这里面的error 就是公式6里面的e(i,j)
          error = R[i * minM + j];
          for (int k = 0; k < minK; ++k)
          {
            //printf("error  %lf pq  %lf\n", error, P[i * K + k] * Q[k * M + j]  );
            error -= P[i * minK + k] * Q[k * minM + j];
            //getchar();
          }
          //更新公式6
          for (int k = 0; k < minK; ++k)
          {
            P[i * minK + k] += alpha * (2 * error * Q[k * minM + j] - beta * P[i * minK + k]);
            updtP[i * minK + k] += alpha * (2 * error * Q[k * minM + j] - beta * P[i * minK + k]);
            Q[k * minM + j] += alpha * (2 * error * P[i * minK + k] - beta * Q[k * minM + j]);
            updtQ[k * minM + j] += alpha * (2 * error * P[i * minK + k] - beta * Q[k * minM + j]);
          }
        }

      }

    }

    //printf("error = %lf\n", error );
    //double loss = 0;
    //计算每一次迭代后的，loss大小，也就是原来R矩阵里面每一个非缺失值跟预测值的平方损失
    double rmse = 0;
    for (int i = 0; i < minN; ++i)
    {
      for (int j = 0; j < minM; ++j)
      {
        //if (R[i * minM + j] > 0)
        {
          double error = 0;
          for (int k = 0; k < minK; ++k)
          {
            error += minP[i * minK + k] * Q[k * minM + j];
          }
          //loss += pow(R[i * M + j] - error, 2);
          rmse += pow(minR[i * minM + j] - error, 2);
          /*
          for (int k = 0; k < K; ++k)
          {
            loss += (beta / 2) * (pow(P[i * K + k], 2) + pow(Q[k * M + j], 2));
          }
          **/
        }
      }
    }
    rmse = sqrt(rmse / (minN * minM));
    //if (loss < 0.001)
    if (rmse < 0.001)
      break;

    if (step % 50 == 0)
      //getchar();
      //cout << "step:\t" << step << "\tloss:" << loss << "\trmse=" << rmse << endl;
      cout << "step:\t" << step << "\trmse=" << rmse << endl;

  }

}


void printBlockPair(Block& pb, Block& qb, int minK)
{
  double rmse = 0.0;
  printf("\n********Below P[%d]*************\n", pb.block_id);
  for (int i = 0 ; i < pb.psz; i++)
  {
    for (int j = 0; j < minK; j++)
    {
      printf("%lf\t", pb.eles[i * minK + j]);
    }
    printf("\n");
  }
  printf("\n+++++++++++Below Q[%d]+++++++++++\n", qb.block_id);
  for (int i = 0 ; i < qb.psz; i++)
  {
    for (int j = 0; j < minK; j++)
    {
      printf("%lf\t", qb.eles[i * minK + j]);
    }
    printf("\n");
  }
  printf("\n++++++++Below Rb++++++++++\n");

  for (int i = 0; i < pb.psz; i++)
  {
    for (int j = 0; j < qb.psz; j++)
    {
      double e = 0;
      for (int k = 0; k < minK; k++)
      {
        e += pb.eles[i * minK + k] * qb.eles[j * minK + k];
      }
      printf("%lf\t", e);
    }
    printf("\n");
  }
  printf("\n++++++++Below R+++++++++++++\n");

  for (int i = 0; i < pb.psz; i++)
  {
    for (int j = 0; j < qb.psz; j++)
    {
      double e = 0;
      for (int k = 0; k < minK; k++)
      {
        e += pb.eles[i * minK + k] * qb.eles[j * minK + k];
      }
      int row_idx = i + pb.sta_idx;
      int col_idx = j + qb.sta_idx;
      printf("%lf\t", R[row_idx * M + col_idx]);
      rmse += (e - R[row_idx * M + col_idx]) *  (e - R[row_idx * M + col_idx]);
    }
    printf("\n");
  }
  printf("\n***************************\n");
  printf("rmse=%lf\n", rmse);

}

void submf(double *minR, Block& minP, Block& minQ, Updates& updateP, Updates& updateQ, int minN, int minM, int minK, int steps = 50, float alpha = 0.0002, float beta = 0.02)
{
  /*
  printf("minN=%d\tminM=%d\n", minN, minM );
  printf("minP:\n");
  minP.printBlock();
  printf("minQ:\n");
  minQ.printBlock();
  **/
  double error = 0;
  double error_m = 0;
  int Psz =  minP.psz * minK;
  int Qsz = minQ.psz * minK;
  updateP.eles.reserve(Psz);
  updateQ.eles.reserve(Qsz);
  updateP.eles.resize(Psz);
  updateQ.eles.resize(Qsz);
  int ii = 0;
  for (ii = 0; ii < Psz; ii++)
  {
    updateP.eles[ii] = 0;
  }
  for ( ii = 0; ii < Qsz; ii++)
  {
    updateQ.eles[ii] = 0;
  }

  /*
  printf("minR:\n");
  for (int i = 0; i < minM; i++)
  {
    for (int j = 0; j < minN; j++)
    {
      printf("%lf\t", minR[i * minM + j]);
    }
    printf("\n");
  }
  **/

  //for (int step = 0; step < steps; ++step)
  {
    for (int i = 0; i < minN; ++i)
    {
      for (int j = 0; j < minM; ++j)
      {

        if (minR[i * minM + j] > 0)
        {
          //printf("idx = %d\n", i * minM + j );
          //这里面的error 就是公式6里面的e(i,j)
          error = minR[i * minM + j];
          //printf("error = %lf\n", error );

          for (int k = 0; k < minK; ++k)
          {
            //error_m -= P[i * minK + k] * Q[k * minM + j];
            error -= minP.eles[i * minK + k] * minQ.eles[j * minK + k];
          }


          //更新公式6
          for (int k = 0; k < minK; ++k)
          {
            //printf("Pidx %d  Qidx %d Psz  %d Qsz %d\n", i * minK + k, j * minK + k, Psz, Qsz );

            updateP.eles[i * minK + k] += alpha * (2 * error * minQ.eles[j * minK + k] - beta * minP.eles[i * minK + k]);
            updateQ.eles[j * minK + k] += alpha * (2 * error * minP.eles[i * minK + k] - beta * minQ.eles[j * minK + k]);

            //printf("Mirror:\n");
            //printf("Pup %lf  =  %lf\n", alpha * (2 * error * minQ.eles[j * minK + k] - beta * minP.eles[i * minK + k]), alpha * (2 * error * Q[k * minM + j] - beta * P[i * minK + k]) );
            //printf("Qup %lf  =  %lf\n", alpha * (2 * error * minP.eles[i * minK + k] - beta * minQ.eles[j * minK + k]), alpha * (2 * error * P[i * minK + k] - beta * Q[k * minM + j]) );

          }

        }
      }
    }
    if (worker_debug)
    {
      printf("COOOCMMM\n");
      printf("\n(((((((((((((((((((\n");
      updateP.printUpdates();
      updateQ.printUpdates();
      printf("\n)))))))))))))))))))\n");
    }

  }



}

void partitionP(double* minP, int minN, int minK, int portion_num,  Block* Pblocks)
{
  int i = 0;
  int height = minN / portion_num;
  int last_height = minN - (portion_num - 1) * height;
  //printf("partitionP  portion_num=%d\n", portion_num);
  int block_size = height * minK;
  int last_size = (last_height * minK);
  //printf("block_size=%d last_size=%d\n", block_size, last_size );
  for (i = 0; i < portion_num; i++)
  {
    Pblocks[i].block_id = i;
    Pblocks[i].data_age = 0;
    //Pblocks[i].eles.clear();
    Pblocks[i].psz = height;
    int sz = block_size;
    int sta_idx = i * height;
    if ( i == portion_num - 1)
    {
      sz = last_size;
      Pblocks[i].psz = last_height;
    }
    Pblocks[i].sta_idx = sta_idx;
    //printf("i=%d sz=%d sta_idx=%d\n", i, sz, sta_idx );
    int j = 0;
    for (j = 0; j < sz; j++)
    {
      Pblocks[i].eles.push_back(minP[sta_idx + j]);
    }
    //Pblocks[i].printBlock();
    //getchar();
  }
  //printf("End\n");
}


void worker_thread(int worker_id)
{
  if (worker_debug)
  {
    printf("worker_id  %d\n",  worker_id);
  }

  int p_block_id = worker_id;
  int q_block_id = worker_id;
  int clock_t = 0;
  int staleness = 0;
  while (1 == 1)
  {
    if (worker_debug && worker_id == 0)
    {
      printf("Td:[%d] clock_t = %d  p_block_id=%d data_age = %d\n", worker_id, clock_t, p_block_id, Pblocks[p_block_id].data_age);
    }

    //getchar();
    if (Pblocks[p_block_id].data_age <= clock_t - staleness - 1\
        || Qblocks[q_block_id].data_age <= clock_t - staleness - 1)
    {
      if (worker_debug && worker_id == 0)
      {
        printf("Td:[%d] NO match sleep ...\n", worker_id);
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
    else
    {

      Pmtx[p_block_id].lock();
      Block pblock = Pblocks[p_block_id];
      Pmtx[p_block_id].unlock();

      Qmtx[q_block_id].lock();
      Block qblock = Qblocks[q_block_id];
      Qmtx[q_block_id].unlock();

      Updates p_update, q_update;
      p_update.block_id = p_block_id;
      q_update.block_id = q_block_id;
      p_update.clock_t = clock_t;
      q_update.clock_t = clock_t;
      /*
      p_update.eles.resize(100);
      q_update.eles.resize(100);
      for (int i  = 0; i < 100; i++)
      {
        p_update.eles[i] = q_update.eles[i] = 0;
      }
      **/

      /*
            if (worker_id != 0)
            {
              break;
            }
            **/

      double* minR = new double[pblock.psz * qblock.psz];
      //printf("minR sz = %d\n", pblock.psz * qblock.psz );

      int x_sta_idx = pblock.sta_idx;
      int y_sta_idx = qblock.sta_idx;
      for (int i = 0; i < pblock.psz; i++)
      {
        for (int j = 0; j < qblock.psz; j++)
        {
          int row_idx = i + pblock.sta_idx;
          int col_idx = j + qblock.sta_idx;
          minR[i * qblock.psz + j] = R[row_idx * M + col_idx];
          //printf("row_id  %d  col_id  %d\n", row_idx, col_idx);
        }
      }

      if (worker_debug && worker_id == 0)
      {
        printf("Before Adjust  Td[%d]", worker_id);
        printBlockPair(pblock, qblock, K);
        printf("Before summf\n");
      }

      submf(minR, pblock, qblock, p_update, q_update, pblock.psz, qblock.psz,  K);

      if (worker_debug)
      {
        printf("\n+++++++++++++++++++++++\n");
        p_update.printUpdates();
        q_update.printUpdates();
        printf("\n+++++++++++++++++++++++\n");
      }


      if (worker_debug && worker_id == 0)
      {
        printf("after Adjust  [Td:%d]", worker_id);
        printBlockPair(pblock, qblock, K);
      }
      delete[] minR;


      int p_tail = tail_ptrs_P[worker_id] % cap;
      ThreadUpdadtesP[worker_id][p_tail] =  p_update;
      tail_ptrs_P[worker_id]++;
      int q_tail = tail_ptrs_Q[worker_id] % cap;
      ThreadUpdadtesQ[worker_id][q_tail] = q_update;
      //printf("p_tail  =%d  q_tail = %d\n", p_tail, q_tail );
      //printf("sze  =%lu  psz=%lu\n", ThreadUpdadtesP[0][0].eles.size(), p_update.eles.size() );
      tail_ptrs_Q[worker_id]++;

      //p_block_id = (p_block_id + 1) % THREAD_NUM;
      q_block_id = (q_block_id + 1) % THREAD_NUM;
      clock_t++;

    }
  }

}

double CalcRMSE(int block_num, int k_width, Block* row_blk, Block* col_blk)
{
  double rmse = 0;
  for (int i = 0; i < block_num; i++)
  {
    for (int j = 0; j < block_num; j++)
    {

    }
  }
  return rmse;
}


int main(int argc, char ** argv)
{

// R[0] = 5, R[1] = 3, R[2] = 0, R[3] = 1, R[4] = 4, R[5] = 0, R[6] = 0, R[7] = 1, R[8] = 1, R[9] = 1;
// R[10] = 0, R[11] = 5, R[12] = 1, R[13] = 0, R[14] = 0, R[15] = 4, R[16] = 0, R[17] = 1, R[18] = 5, R[19] = 4;

  cout << "R矩阵(N行M列)" << endl;

  srand(1);
  for (int i = 0; i < N; ++i)
  {
    for (int j = 0; j < M; ++j)
    {
      R[i * M + j] = ((float)(rand() % 1000)) / 100;
      //cout << R[i * M + j] << ',';
    }
    //cout << endl;
  }

//初始化P，Q矩阵，这里简化了，通常也可以对服从正态分布的数据进行随机数生成

  //srand(time(0));
  srand(1);
  for (int i = 0; i < N; ++i)
  {
    for (int j = 0; j < K; ++j)
    {
      P[i * K + j] = ((float)(rand() % 1000)) / 100;
      //P[i * K + j] = i * K + j;
    }
  }

  for (int i = 0; i < K; ++i)
  {
    for (int j = 0; j < M; ++j)
    {
      Q[i * M + j] = ((float)(rand() % 1000)) / 100;
      //Q[i * M + j] = i * M + j;
    }
  }
  unsigned num_cpus = std::thread::hardware_concurrency();
  //THREAD_NUM = num_cpus;
  THREAD_NUM = 4;
  printf("Lanching %d threads\n", THREAD_NUM);
  for (int i = 0; i < THREAD_NUM; i++)
  {
    ThreadUpdadtesP[i].reserve(cap);
    ThreadUpdadtesP[i].resize(cap);
    head_ptrs_P[i] = 0;
    tail_ptrs_P[i] = 0;
    ThreadUpdadtesQ[i].reserve(cap);
    ThreadUpdadtesQ[i].resize(cap);
    head_ptrs_Q[i] = 0;
    tail_ptrs_Q[i] = 0;
  }
  Pblocks = new struct Block[THREAD_NUM];
  Qblocks = new struct Block[THREAD_NUM];
  //printf("Before Partition\n");
  partitionP(P, N, K, THREAD_NUM, Pblocks);
  //printf("After Partition\n");

  for (int i = 0; i < THREAD_NUM; i++)
  {
    Pblocks[i].printBlock();
  }

  //getchar();

  for (int i = 0; i < K; ++i)
  {
    for (int j = 0; j < M; ++j)
    {
      Qt[j * K + i] =  Q[i * M + j];
      printf("%lf\t", Q[i * M + j] );
    }
    printf("\n");
  }
  partitionP(Qt, M, K, THREAD_NUM, Qblocks);

  for (int i = 0; i < THREAD_NUM; i++)
  {
    Qblocks[i].printBlock();
  }

  //getchar();
  //matrix_factorization(R, P, Q, N, M, K);

  //return 0;
  int worker_id = 0;
  vector<thread> thread_vec ;
  for (worker_id = 0; worker_id < THREAD_NUM; worker_id++)
  {
    std::thread worker(worker_thread, worker_id);
    worker.detach();
  }
  int global_clock_t = 0;
  main_debug = false;
  worker_debug = false;

  while (1 == 1)
  {
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    {
      int i = 0;
      int j = 0;
      for (i = 0; i < THREAD_NUM; i++ )
      {
        /*
                while (!(head_ptrs_P[i] < tail_ptrs_P[i] &&  head_ptrs_Q[i] < tail_ptrs_Q[i]))
                {
                  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
                  printf("sleeping\n");
                }
        **/

        if (main_debug && head_ptrs_P[i] < tail_ptrs_P[i] &&  head_ptrs_Q[i] < tail_ptrs_Q[i])
        {
          printf("Before Adjust \n");

          int p_ele_idx = head_ptrs_P[i] % cap;
          int pbid = ThreadUpdadtesP[i][p_ele_idx].block_id;
          int q_ele_idx = head_ptrs_Q[i] % cap;
          int qbid = ThreadUpdadtesQ[i][q_ele_idx].block_id;

          printBlockPair(Pblocks[pbid], Qblocks[qbid], K);
        }

        if (head_ptrs_P[i] < tail_ptrs_P[i])
        {

          int ele_idx = head_ptrs_P[i] % cap;
          int pbid = ThreadUpdadtesP[i][ele_idx].block_id;
          //printf("pbid = %d\n", pbid);

          Pmtx[pbid].lock();
          //printf("Pupdates ele_idx=%d\n", ele_idx);

          for (j = 0; j < Pblocks[pbid].eles.size(); j++)
          {
            Pblocks[pbid].eles[j] += ThreadUpdadtesP[i][ele_idx].eles[j];
            //printf("%lu\t", ThreadUpdadtesP[i][ele_idx].eles.size());
          }
          if (main_debug)
          {
            printf("\n");
            ThreadUpdadtesP[i][ele_idx].printUpdates();
          }


          Pblocks[pbid].data_age++;
          //printf("MergeUpdate pbid =%d  age =%d\n", pbid, Pblocks[pbid].data_age);

          Pmtx[pbid].unlock();

          head_ptrs_P[i]++;


        }

        if (head_ptrs_Q[i] < tail_ptrs_Q[i])
        {

          int ele_idx = head_ptrs_Q[i] % cap;
          int qbid = ThreadUpdadtesQ[i][ele_idx].block_id;
          //printf("qbid = %d\n", qbid);
          Qmtx[qbid].lock();
          //printf("Qupdates\n");
          for (j = 0; j < Qblocks[qbid].eles.size(); j++)
          {
            Qblocks[qbid].eles[j] += ThreadUpdadtesQ[i][ele_idx].eles[j];
            //printf("%lf\t", ThreadUpdadtesQ[i][ele_idx].eles[j]);
          }
          if (main_debug)
          {
            printf("\n");
            ThreadUpdadtesQ[i][ele_idx].printUpdates();
          }

          Qblocks[qbid].data_age++;
          //printf("MergeUpdate qbid =%d  age =%d\n", qbid, Qblocks[qbid].data_age);
          Qmtx[qbid].unlock();
          head_ptrs_Q[i]++;


        }

        //printf("FIN\n");

        if (head_ptrs_P[i] <= tail_ptrs_P[i] &&  head_ptrs_Q[i] <= tail_ptrs_Q[i])
        {
          printf("After Adjust \n");
          int p_ele_idx = (head_ptrs_P[i] - 1) % cap;
          int pbid = ThreadUpdadtesP[i][p_ele_idx].block_id;
          int q_ele_idx = (head_ptrs_Q[i] - 1) % cap;
          int qbid = ThreadUpdadtesQ[i][q_ele_idx].block_id;

          printBlockPair(Pblocks[pbid], Qblocks[qbid], K);
          //getchar();
        }



      }

    }


  }

  return 0;

}