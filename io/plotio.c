/* DEDISbench
 * (c) 2010 2017 INESC TEC and U. Minho
 * Written by M. Freitas
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "plotio.h"
#include "../utils/utils.h"

static int find_bucket(unsigned long long int i){
	return order_of_magnitude(i);
}


int write_access_data(const uint64_t* accessesarray, struct user_confs* conf, char* id){
	int ret = 0;
	char plotfile_dir[256] = "results/accesses/";
	char cumul_acc_file_dir[256] = "results/accesses/";
	char acc_file_dir[256] = "results/accesses/";
	char cumulaccfile[128];
	char plotfile[128];
	
	FILE* fpp;
	FILE* fpcumul;
	FILE* fpplot;

	uint64_t pos_touched = 0;
	uint64_t bytes_processed = 0;

  	strcat(conf->accessfilelog,id);
	strcpy(cumulaccfile, conf->accessfilelog);
	strcat(cumulaccfile, "cumul");
	strcat(cumul_acc_file_dir, cumulaccfile);
	
	strcpy(plotfile, cumulaccfile);
	strcat(plotfile, "plot");
	strcat(plotfile_dir, plotfile);
 
 	strcat(acc_file_dir, conf->accessfilelog);	
	//print distribution file
  	fpp=fopen(acc_file_dir,"w");
	if(!fpp)
	{
		ret = 1;
		return ret;
	}
	fpcumul=fopen(cumul_acc_file_dir,"w");
	if(!fpcumul)
	{
		ret = 1;
		return ret;
	}
	fpplot=fopen(plotfile_dir, "w");
	if(!fpplot)
	{
		ret = 1;
		return ret;
	}
	
	write_plot_file_accesses(fpplot, cumulaccfile);
	fclose(fpplot);

	// add name of mode
	char *mode;
	if(conf->accesstype == SEQUENTIAL)
	  mode = "sequential";
	else if(conf->accesstype == UNIFORM)
	  mode = "uniform";
	else
	  mode = "hotspot";

	uint64_t iter;
	// [1:5[[5:10[[10:50[[50:100[[100:500[[500:1000[
	//P ---10¹---  ------10²----  ------10³--------
	//     B1            B2             B3
	//   1    2      3      4        5        6

	//unsigned long long int acs[8];
	//memset(acs, 0, sizeof(unsigned long long int)*8);
	int acs_len = 8;
	unsigned long long int* acs = calloc(acs_len, sizeof(unsigned long long int));
	int init = 1, final = 10;

	/* agreggation */
	for(iter=0;iter<conf->totblocks;iter++){
		if(accessesarray[iter]>0){
			pos_touched+=1;
			bytes_processed+=conf->block_size*accessesarray[iter];
		}
			
		fprintf(fpp,"%llu %llu\n",(unsigned long long int) iter, (unsigned long long int) accessesarray[iter]);

		int bucket = find_bucket((unsigned long long int) accessesarray[iter]);
		int power = powr(10, bucket);
		int arr_pos;
		
		if((unsigned long long int) accessesarray[iter] >= power){
			bucket++;
			power *= 10;
		}

		if((unsigned long long int) accessesarray[iter] >= (power/2)){
			arr_pos = bucket*2;
		}else{
			arr_pos = (bucket*2)-1;
		}

		if(arr_pos >= acs_len)
		{
			acs = realloc(acs, sizeof(unsigned long long int)*acs_len*2);
			memset(acs + (acs_len), 0, sizeof(unsigned long long int)*acs_len);
			acs_len *= 2;
		}
		acs[arr_pos]++;
	}

	/* printing agreggated data */
	fprintf(fpcumul, "\t%s\n",mode);
	int i = 1;
	while(i < acs_len && acs[i]){
	  if(acs[i]){
		fprintf(fpcumul, "[%d,%d[ %llu\n", init, final>>1, acs[i++]);
	  }
	  else 
		  i++;
	  
	  if(i < acs_len && acs[i]){
		fprintf(fpcumul, "[%d,%d[ %llu\n", final>>1, final, acs[i++]);
	  }
	  else
		  i++;
	  init = final;
	  final *= 10;
  	}
	fclose(fpcumul);
	fclose(fpp);

	if(conf->iotype==READ){
		printf("Process touched %llu blocks totalling %llu MB. Process read %llu MB (including block reread)\n", (unsigned long long int) pos_touched, (unsigned long long int) (pos_touched*conf->block_size)/1024/1024, (unsigned long long int) bytes_processed/1024/1024);
	}else{
		printf("Process touched %llu blocks totalling %llu MB. Process wrote %llu MB (including block rewrite)\n", (unsigned long long int) pos_touched, (unsigned long long int) (pos_touched*conf->block_size)/1024/1024, (unsigned long long int) bytes_processed/1024/1024);
	}

	return ret;
}



static void write_snaplat_data(struct stats* stat, FILE* f, FILE* fcompat){
	
	fprintf(f,"%llu 0 0\n",(unsigned long long int)stat->beginio);
	for(int index=0;index < stat->iter_snap; index++){
		if(f)
			fprintf(f, "%llu %.3f %f\n", (unsigned long long int) stat->snap_time[index], stat->snap_latency[index], stat->snap_ops[index]);
		if(fcompat)
			fprintf(fcompat, "%d %.3f %f\n", (index)*30,stat->snap_latency[index],stat->snap_ops[index]);
	}

}



static void write_snapthr_data(struct stats* stat, FILE* f, FILE* fcompat){
	
	fprintf(f,"%llu 0 0\n",(unsigned long long int)stat->beginio);
	for(int index=0 ;index < stat->iter_snap; index++){
		if(f)
			fprintf(f, "%llu %.3f %f\n", (unsigned long long int) stat->snap_time[index], stat->snap_throughput[index], stat->snap_ops[index]);
		if(fcompat)
			fprintf(fcompat, "%d %.3f %f\n", (index)*30,stat->snap_throughput[index],stat->snap_ops[index]);
	}

}



int write_latency_throughput_snaps(struct stats* stat, struct user_confs* conf, char* id)
{
	  int ret = 0;
	  
	  //dir for raw file (unprocessed data) 
	  char snap_thr_dir_out[256] = "./results/latthr/";

	  //dir for processed data file, to be used by
	  //the gnuplot
	  char snap_thr_dir_compat[256] = "./results/latthr/";
	  
	  char snapthrname[PATH_SIZE];
	  char snapthrfmt[PATH_SIZE];
	  strcpy(snapthrname,conf->printfile);
	  strcat(snapthrname,"snapthr");
	  if(conf->iotype==WRITE){
		  strcat(snapthrname,"write");
	  }
	  else{
		  strcat(snapthrname,"read");
	  }
	  
	  strcat(snapthrname,id);
	  strcat(snap_thr_dir_out, snapthrname);

	  strcpy(snapthrfmt, snapthrname);
	  strcat(snapthrfmt, "compat");
	  strcat(snap_thr_dir_compat, snapthrfmt);

	  // latency 
	  char snap_lat_dir_out[256] = "./results/latthr/";
	  char snap_lat_dir_compat[256] = "./results/latthr/";
	  char snaplatname[PATH_SIZE];
	  char snaplatfmt[PATH_SIZE];
	  strcpy(snaplatname,conf->printfile);
	  strcat(snaplatname,"snaplat");
	  if(conf->iotype==WRITE){
	  	  strcat(snaplatname,"write");
	  }
	  else{
	  	  strcat(snaplatname,"read");
	  }
	 
	  strcat(snaplatname,id);
	  strcat(snap_lat_dir_out, snaplatname);
	  
	  strcpy(snaplatfmt, snaplatname);
	  strcat(snaplatfmt, "compat");
	  strcat(snap_lat_dir_compat, snaplatfmt);
	 
	  // write latency data 
	  //printf("Opening %s...", snap_lat_dir_out);
	  FILE* pf=fopen(snap_lat_dir_out,"a");
	  if(!pf){
		  ret = 1;
		  //printf("ERROR\n");
		  return ret;
	  }
	  //printf("DONE\n");
	  //fprintf(pf,"%llu 0 0\n", (unsigned long long int)stat->beginio);
	  
	  //printf("Opening %s...", snap_lat_dir_compat);
	  FILE* pfcompat = fopen(snap_lat_dir_compat,"w");
	  if(!pfcompat){
		  ret = 1;
		  //printf("ERROR\n");
		  return ret;
	  }
	  //printf("DONE\n");
	  write_snaplat_data(stat, pf, pfcompat); 
	  fclose(pf);
	  fclose(pfcompat);
	
	  // write throughput data 
	  pf=fopen(snap_thr_dir_out,"a"); 
	  if(!pf){
		  ret = 1;
		  return ret;
	  }
	  pfcompat = fopen(snap_thr_dir_compat,"w");
	  if(!pfcompat){
		  ret = 1;
		  return ret;
	  }

	  //fprintf(pf,"%llu 0 0\n",(unsigned long long int)stat->beginio);
	  
	  write_snapthr_data(stat, pf,pfcompat); 
	  fclose(pf);
	  fclose(pfcompat);
	 
	  // dir of plot file
	  char snap_plot_dir[256] = "results/latthr/";
	  // plot file name
	  char plotfile[PATH_SIZE];
	  strcpy(plotfile,conf->printfile);
	  strcat(plotfile,id);
	  strcat(plotfile,"plot");
	  // completing the directory
	  strcat(snap_plot_dir, plotfile);
	  
	  // write plot file 
	  pf = fopen(snap_plot_dir, "w");
	  if(!pf){
		  ret = 1;
		  return ret;
	  }
	  write_plot_file_latency_throughput(pf,conf->distfile, snaplatfmt, snapthrfmt);
	  fclose(pf);
	  
	  return ret;
}



void write_plot_file_distribution(FILE* f, char* distcumulfile, char* distfile)
{
	fprintf(f, "set grid ytics lt 0 lw 1 lc rgb \"#bbbbbb\"\n");
	fprintf(f, "set style data histogram\n");
	fprintf(f, "set style histogram cluster gap 2\n");
	fprintf(f, "set style fill solid\n");
	fprintf(f, "set xlabel \"Number of duplicates\"\n");
	fprintf(f, "set ylabel \"Number of blocks\"\n");
	fprintf(f, "set logscale y\n");
	fprintf(f, "set boxwidth 0.8\n");
	fprintf(f, "set xtic scale 0 font \"1\"\n");
	fprintf(f, "set xtics rotate by 45 right\n");
	fprintf(f, "set key outside\n");
	fprintf(f, "set key top horizontal\n");
	fprintf(f, "plot '%s' using 2:xtic(1) ti '%s' noenhanced fillstyle pattern 4 lc rgb \"#e70000\"\n", distcumulfile, distfile);
}



void write_plot_file_latency_throughput(FILE* f, char* distfile, char* latfile, char* thrfile)
{
	fprintf(f, "set multiplot layout 2,1 rowsfirst title \"Latency and Throughput Plots ('%s' data set)\" noenhanced\n", distfile);
	fprintf(f, "set grid ytics lt 0 lw 1 lc rgb \"#bbbbbb\"\n");
	fprintf(f, "set offsets 0,30,0.07,0\n");
//	fprintf(f, "set label 1 'latency' at graph 0.25,0.25 font '8'\n");
//	fprintf(f, "set xlabel \"Time(s)\"\n");
	fprintf(f, "set ylabel \"Latency (ms)\"\n");
	fprintf(f, "set yrange [0.0:*]\n");
	fprintf(f, "set xtics rotate by 45 right\n");
	fprintf(f, "set xtics nomirror\n");
	fprintf(f, "set ytics nomirror\n");
	fprintf(f, "set xrange [0.0:*]\n");
	fprintf(f, "set pointsize 1.0\n");
	fprintf(f, "set key off\n");
	fprintf(f, "plot '%s' using 1:2 with linespoints lc rgb 'blue' ti 'Latency (miliseconds)'#,\"\" using 1:2:(sprintf(\"%s\",$3)) with labels offset char 0,1 notitle\n", latfile, "\%d");
//	fprintf(f, "set label 1 'throughput' at graph 0.92,0.9 font '8'\n");
	fprintf(f, "set grid ytics lt 0 lw 1 lc rgb \"#bbbbbb\"\n");
	fprintf(f, "set offsets 0,30,1000,0\n");
	fprintf(f, "set xlabel \"Time(s)\"\n");
	fprintf(f, "set ylabel \"Throughput (blocks/s)\"\n");
	fprintf(f, "set autoscale y\n");
	fprintf(f, "set xrange [0.0:*]\n");
	fprintf(f, "set pointsize 1.0\n");
	fprintf(f, "plot '%s' using 1:2 with linespoints lc rgb 'red' ti 'Throughput (blocks/second)'#, \"\" using 1:2:(sprintf(\"%s\",$3)) with labels offset char 0,1 notitle\n", thrfile, "\%d");
	fprintf(f, "unset multiplot\n");
}



void write_plot_file_accesses(FILE* f, char* cumulaccfile)
{
	fprintf(f, "set grid ytics lt 0 lw 1 lc rgb \"#bbbbbb\"\n");
	fprintf(f, "set style data histogram\n");
	fprintf(f, "set style histogram cluster gap 1\n");
	fprintf(f, "set style fill solid\n");
	fprintf(f, "set xtics rotate by 45 right\n");
	fprintf(f, "set ytics nomirror\n");
	fprintf(f, "set xlabel \"Number of accesses\"\n");
	fprintf(f, "set ylabel \"Number of Blocks\"\n");
	fprintf(f, "set logscale y\n");
	fprintf(f, "#set key at screen 0.90,screen 1 top right horizontal font \"Times-Roman, 9\"\n");
	fprintf(f, "set key outside\n");
	fprintf(f, "set key top horizontal\n");
	fprintf(f, "set boxwidth 0.8\n");
	fprintf(f, "set xtic scale 0 font \"1\"\n");
	fprintf(f, "plot '%s' using 2:xtic(1) ti col fillstyle pattern 4 lc rgb \"#e70000\"\n", cumulaccfile);
}
