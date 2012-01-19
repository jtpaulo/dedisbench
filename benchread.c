/* DEDIS bench
 * (c) 2010 2010 U. Minho. Written by J. Paulo
 */

//versão contador unico
#define _GNU_SOURCE
#define _FILE_OFFSET_BITS 64 

#include <unistd.h>
#include <stdio.h>
#include <signal.h>
#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <math.h>
#include "SFMT.c"
#include <sys/time.h>
#include <time.h>
#include <sys/wait.h>

//nº total de ids de blocos serem escritos -> tamanho da imagem esparça se multiplicado por 4096...
//40Gb
//uint32_t totblocks = 10485760;
uint32_t totblocks = 2097152;

//nº total de escritas a serem feitas
//40Gb
//uint32_t totwrites = 2097152;
//uint32_t totwrites = 10485760;


//nº blocos que possuem duplicados -> retirado do Homer
int nd = 1839041;

//nº blocos que não possuem duplicados -> retirado do homer
int nu = 22610369;

//array com dados do Homer possui para cada bloco com duplicados o numero de duplicados que tem.
uint32_t stats[1839041];

//arayy que possui soma da pos anterior sum[n] = stats[n]+sum[n-1]; -> usado para depois ter probabilidades de ter duplicados
uint32_t sum[1839041];

//debug

//resultados das escritas conteudo da resultado tipo homer 
//uint32_t cont[10000000];

//dá nº de escritas em cada id de bloco...
//uint32_t pos[2621440];

//debug
//int contnu;
//conta blocos unicos
int unico=0;
//conta blocos duplicados
int dupl=0;

//gera aleatorio entre 0 e max.
uint32_t genrand(int max){

    uint32_t t = gen_rand32();
    

    //isto e feito para ser equiprovavel, se t for maior que o ultimo nº por qual INT32-MAX e divisivel com resto 0 temos de calcular random de novo
    //senao o que acontece é que deixa de ser equiprovavel
    while(t>=INT32_MAX/max * max){
          
          t = gen_rand32();

    }

    //this way we only et a random from 0 to max
    return t%max;
}

//carrega stats homer 
void load_stats(){

    int statscont =0;

    int i =0;
    int j =0;

    FILE *fp = fopen("block4","r");
    char line[30];
  
    bzero(line,sizeof(line));

    if(fp){
    while(fgets(line, sizeof(line), fp)) {
         
          char dup[10];

          //get phyadd
          i =0;
          while(line[i]!=' '){
            dup[i]=line[i];
            i++;
          }
          dup[i]='\0';
          i++;
          
          char nblocks[10];

          //get hash sum
          j=0;
          while(line[i]!='\n'){
            nblocks[j]=line[i];
            i++;
            j++;
          }
          nblocks[j]='\0';
         
          uint64_t dn;
          uint64_t nb;

          //convert
          dn = atoll(dup) + 1 ; // aqui somamos +1 porque queremos no array o nº de occurencias e não od e duplicados  
          nb = atoll(nblocks);          
         
          while(nb>0){
            stats[statscont] = dn;           
 
            statscont++;
            nb--;
          }
           

          bzero(nblocks,sizeof(nblocks));
          bzero(dup,sizeof(dup));
          
                   
     }    
      
  fclose(fp);    
  }
  else{
   printf("falhou a abrir o ficheiro \n");
   
  }



}

//after loading the block file we must calculate the stats array...
void init(){

  int i;

  sum[0]=stats[0];
  
  for(i=1;i<nd;i++){

    sum[i]=sum[i-1]+stats[i];
  }

  printf("%d\n",sum[nd-1]);

}



//binary search done to find the block content that we want to generate
uint32_t search(uint32_t value,int low, int high, uint32_t res){

   
   if (high < low)
       return res;

   uint32_t mid = low + ((high - low) / 2);  // Note: not (low + high) / 2 !!
   //printf("%llu  low %d high %d \n",mid,low,high);

   if (sum[mid] > value){
        //printf("value %llu\n",sum[mid]);
        res = mid;
        return search(value, low, mid-1, res);
   }
   else { 
    
     if (sum[mid] < value)
        return search(value, mid+1, high, res);

     else
        return mid+1; // found
   }
  

}


//function that calculates the contetn to generate that resembles our distribution from Homer
uint32_t contescrita(int contnu,char* nomec){

  //gera um nº aleatorio entre 0 e nº de blocos
  //é aqui que esta mall.....
  uint32_t r = genrand(29530586);

  //printf("%u\n",sum[nd-1]);

  //se nº maior que sum[nd-1] entao é unico
  if (r>=sum[nd-1]) {
      unico++;
      //codigo a mais...
      
      //r toma valor de contador unico
      r = contnu;
      contnu++;

      return r; }
  else {
     //e duplicado faz procura por qual o cont a gerar
     uint32_t res = search(r,0,nd-1,0LLU);
     //if(res>=nd){printf("maior ou igual ??? %d res %d r\n",res,r);}
     dupl++;
     
     //FILE *fp = fopen(nomec,"a");
     //fprintf(fp,"%d\n",res);
     //fclose(fp);

     return res; 
  } 
}

//calculo de pos de escrita segue tpcc
//NURand(A, x, y) = (((random(0, A) | random(x, y)) + C) % (y - x + 1)) + x
uint64_t posescrita(uint32_t totb,char *nomep){

  uint32_t a = 9000;
  uint32_t x = 0;

  uint32_t y = totb-1;
  //TODO c é gerado aqui dentro ou será fora....
  uint32_t c = genrand(a+1);

  uint32_t res = ((( genrand(a) | genrand(y)) + c) % (y - x + 1)) + x ;

  
  if(res>totb-1){printf("erro %d res\n",res);}

  
  //FILE *fp = fopen(nomep,"a");
  //fprintf(fp,"%d\n",res);
  //fclose(fp);

  uint64_t resf = res*4096LL;
  //if(resf>4294332416LL){printf("e maior\n");}
  
  return resf;
}

static struct timeval base;

long lap_time() {
	struct timeval tv;
	long delta;

	gettimeofday(&tv, NULL); 
	delta = (tv.tv_sec-base.tv_sec)*1e6+(tv.tv_usec-base.tv_usec);

        base = tv;

	return delta;
}

void idle(long quantum) {
	usleep(quantum);
}

void run(int procid,int nproc, double ratio,int idmaq){
  
  struct timeval tim;

  gettimeofday(&base, NULL); 

  char nome[20];
  char id[2];
  char idm[2];
  sprintf(id,"%d",procid);
  sprintf(idm,"%d",idmaq);
  strcpy(nome,"escrita");
  strcat(nome,id);

  //descritor do block device onde vamos escrever...
  int fdi = open(nome, O_RDWR | O_LARGEFILE | O_CREAT);
  if(fdi==-1) perror("");

  bzero(nome,sizeof(nome));
  strcpy(nome,"tempo");
  strcat(nome,id);
  strcat(nome,idm);
 
  FILE *ft = fopen(nome,"w");
  
  uint32_t totb = totblocks/nproc;
   
  //int z;

  int contnu = 10000000;
  contnu = idmaq * contnu + contnu;
  contnu = (10000000/nproc)*procid + contnu;

  printf("%d %d\n",contnu,totb);
  double t=1;
  int bt=0;
  

  //logs...
  char nomec[20];
  bzero(nomec,sizeof(nomec));
  strcpy(nomec,"cont");
  strcat(nomec,id);
  strcat(nomec,idm);
 
  char nomep[20];
  bzero(nomep,sizeof(nomep));
  strcpy(nomep,"pos");
  strcat(nomep,id);
  strcat(nomep,idm);
  
  
  gettimeofday(&tim, NULL);
  int inicio=tim.tv_sec;
  printf("antes escritas\n");   
  int fim = inicio+300;

  while(inicio<fim){ 
  //while(bt<1000){ 
  
    //printf("z %d\n",z);
    //anotar tempo
    
    double f=bt*nproc;
    //double d=t/100;
    double d = t;

    assert(f>=0);
    assert(d>0);
    //printf("%f %f %f %f\n",f/d,f,d,t);

    //printf("aqui %d bt \n",bt);
    if (f/d<ratio) {
       
        //printf("vou escrever\n");
        //gettimeofday(&tim, NULL);
        //int ti=tim.tv_sec*1000000+(tim.tv_usec);

 
        uint32_t rescont = contescrita(contnu,nomec);
        //if(rescont<nd){
         //cont[rescont]++;
         //aux++;
        //}

        uint64_t offs = posescrita(totb,nomep);
        //if(offs>10737414144LL){printf("erro %llu offs\n",offs);}
        //if(offs>4294332416LL){printf("e maior\n");}
        //pos[offs]++;

        char buf[4096];
        int bufp = 0;
        for(bufp=0;bufp<4096;bufp++){
          buf[bufp] = 'a';
        }
        //bzero(buf,sizeof(buf));
        sprintf(buf,"%d",rescont);
   
        
	int res = pwrite(fdi,buf,4096,offs);
        if(res ==0 || res ==-1){printf("pwrite esta mal %llu\n",offs); perror("");}
    
        
        bt++;
       
    } else {
       //printf("vou dormir\n");
       idle(4000);
    }
    
    t+=lap_time();

    if((bt%100000)==0){
       printf("bt %d %d\n",bt,procid);

    }

    
     gettimeofday(&tim, NULL);
     inicio=tim.tv_sec;   
  }

   
  printf("antes sleep\n"); 
  sleep(1200);

  gettimeofday(&tim, NULL);
  inicio=tim.tv_sec;
  printf("antes leituras\n");  
  fim = inicio+1800; 

  while(inicio<fim){ 
   
    //uint32_t rescont = contescrita(contnu,nomec);
       
    uint64_t offs = posescrita(totb,nomep);
       
    char buf[4096];
    //int bufp = 0;
    //for(bufp=0;bufp<4096;bufp++){
        //buf[bufp] = 'a';
    //}*/
    //bzero(buf,sizeof(buf));
    //sprintf(buf,"%d",rescont);
   
    gettimeofday(&tim, NULL);
    int t1=tim.tv_sec*1000000+(tim.tv_usec);

    int res = pread(fdi,buf,4096,offs);
    if(res ==0 || res ==-1){printf("pread esta mal %llu\n",offs); perror("");}
    
    gettimeofday(&tim, NULL);
    int t2=tim.tv_sec*1000000+(tim.tv_usec);
    int t2s = tim.tv_sec;  
  
    fprintf(ft,"%d %d\n", t2-t1,t2s);
    bt++;
       
    if((bt%100000)==0){
       FILE* fpa = fopen("outscr","w");
       fprintf(fpa,"btr %d %d\n",bt,procid);
       fclose(fpa);

    }

     gettimeofday(&tim, NULL);
     inicio=tim.tv_sec;   
  }


  fclose(ft);
  close(fdi);  

  printf("unico %d, dup %d, id %d, bt %d\n",unico,dupl,procid,bt);
 
}


int main(int argc, char *argv[]){

  
  //int aux;
  int idmaq = atoi(argv[1]);
  //este id faz com que se possa gerar blocos unicos entre vms... senao eram sempre dplicados
  

  //seedd
  

  load_stats();
  init();  
  
  printf("%d\n",sum[1839040]);
  
  int nproc =4;
  double ratio = atof(argv[3])/1000000.0;
  printf("ratio %f\n",ratio);

  int i,stop;
  stop = 0;
  for(i=0;i<nproc && stop==0;i++){
    if (fork()==0) {

        init_gen_rand(atoi(argv[2]+i));

        printf("vou correr run %d\n",i);
        stop=1;
	run(i, nproc, ratio,idmaq);
    }
  }

    
    
  if(fork()!=0){
    for(i=0;i<nproc;i++)
      wait(NULL);
  }
  return 0;
  
}


