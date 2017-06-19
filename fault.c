/* DEDISbench
 * (c) 2010 2010 U. Minho. Written by J. Paulo
 */

#include "fault.h"
#include "random.h"
#include <string.h>
#include <stdlib.h>



int next_failure(char* buf, int fault_dist, struct duplicates_info *info, uint64_t block_size){

  struct block_info bl;
  uint64_t dupcontent;

  switch(fault_dist){
    case DIST_GEN:
      dupcontent = next_block(info, &bl);
      if(dupcontent==1){
        printf("Injecting failure for content %llu\n", (unsigned long long int)bl.cont_id);
        get_block_content(buf, bl, block_size);
      }
      else{
        get_block_content(buf, info->last_unique_block, block_size);
        printf("Injecting failure for content %llu\n", (unsigned long long int)info->last_unique_block.cont_id);
      
      }
    break;
    case TOP_DUP:
      bl.procid=-1;
      bl.ts=-1;
      bl.cont_id=info->topblock;
      printf("Injecting failure for content %llu\n", (unsigned long long int)info->topblock);
      get_block_content(buf, bl, block_size);
      break;
    case BOT_DUP:
      bl.procid=-1;
      bl.ts=-1;
      bl.cont_id=info->botblock;
      printf("Injecting failure for content %llu\n", (unsigned long long int)info->botblock);
      get_block_content(buf, bl, block_size);
      break;
    case UNIQUE:
      printf("Injecting failure for content %llu\n", (unsigned long long int)info->last_unique_block.cont_id);
      get_block_content(buf, info->last_unique_block, block_size);
      break;
    default:
      perror("Unknown fault distribution\n");
      exit(0);
      break;
  }


  return 0;
}

int inject_failure(int fault_type, int fault_dist, struct duplicates_info *info, uint64_t block_size){

  printf("injecting failure\n");

  char content[block_size];

  next_failure(content, fault_dist, info, block_size);

  printf("Injecting failure with type %d and dist %d\n", fault_type, fault_dist);

  //TODO: call fault layer
  //apply_failure(content, fault_type);
  //

  return 0;
}

//Aux function to split the string with multiple faults
int fault_split(char* a_str, struct user_confs *conf){
    char* tmp = a_str;
    int nr_faults=0;
   
    //starts with one because one element does not need the comma
    while (*tmp)
    {
        if (',' == *tmp)
        {
            nr_faults++;
        }
        tmp++;
    }
    nr_faults++;

    conf->fconf = malloc(sizeof(struct fault_conf)*nr_faults);

    char* token = strtok(a_str, ",");

    int i=0;
    while (token)
    {
        char type[20];
        char when[20];
        char dist[20];
        int sep=0;
        int iter=0;
        char *info=strdup(token);

        while(*info){
          if(*info == ':'){
            sep++;
            iter=0;
          }else{
            switch(sep){
              case 0:
                type[iter]=*info;
                break;
              case 1:
                dist[iter]=*info;
              default:
                when[iter]=*info;
                break;
            }
            iter++;
          }
          info++;
        }

        conf->fconf[i].fault_type=atoi(type);
        conf->fconf[i].fault_dist=atoi(dist);
        conf->fconf[i].when=atoi(when);
        printf("Arg is type %d, dist %d, when %d\n",conf->fconf[i].fault_type, conf->fconf[i].fault_dist, conf->fconf[i].when);
        token = strtok(NULL, ",");
        i++;
        
    }


    return nr_faults;

}
