
#include <stdlib.h>

#include "bon_time.h"
#include "duration.h"
#include <time.h>
#include <string.h>

void BonTimer::start()
{
  m_dur.start();
  m_cpu_dur.start();
}
void BonTimer::stop_and_record(tests_t test)
{
  m_delta[test].Elapsed = m_dur.stop();
  m_delta[test].CPU = m_cpu_dur.stop();
}

void BonTimer::add_delta_report(report_s &rep, tests_t test)
{
  if(m_delta[test].CPU == 0.0)
  {
    m_delta[test].FirstStart = rep.StartTime;
    m_delta[test].LastStop = rep.EndTime;
  }
  else
  {
    m_delta[test].FirstStart = min(m_delta[test].FirstStart, rep.StartTime);
    m_delta[test].LastStop = max(m_delta[test].LastStop, rep.EndTime);
  }
  m_delta[test].CPU += rep.CPU;
  m_delta[test].Elapsed = m_delta[test].LastStop - m_delta[test].FirstStart;
  m_delta[test].Latency = max(m_delta[test].Latency, rep.Latency);
}

BonTimer::BonTimer()
 : m_type(txt)
 , m_concurrency(1)
{
  Initialize();
}

void
BonTimer::Initialize()
{
  for(int i = 0; i < TestCount; i++)
  {
    m_delta[i].CPU = 0.0;
    m_delta[i].Elapsed = 0.0;
    m_delta[i].Latency = 0.0;
  }
  random_source.reset();
}

void
BonTimer::add_latency(tests_t test, double t)
{
  m_delta[test].Latency = max(m_delta[test].Latency, t);
}

int BonTimer::print_cpu_stat(tests_t test)
{
  if(m_delta[test].Elapsed == 0.0)
  {
    if(m_type == txt)
      fprintf(m_fp, "    ");
    else
      fprintf(m_fp, ",");
    return 0;
  }
  if(m_delta[test].Elapsed < MinTime)
  {
    if(m_type == txt)
      fprintf(m_fp, " +++");
    else
      fprintf(m_fp, ",+++");
    return 0;
  }
  int cpu = int(m_delta[test].CPU / m_delta[test].Elapsed * 100.0);
  if(m_type == txt)
    fprintf(m_fp, " %3d", cpu);
  else
    fprintf(m_fp, ",%d", cpu);
  return 0;
}

int BonTimer::print_stat(tests_t test, int test_size)
{
  if(m_delta[test].Elapsed == 0.0)
  {
    if(m_type == txt)
      fprintf(m_fp, "      ");
    else
      fprintf(m_fp, ",");
  }
  else if(m_delta[test].Elapsed < MinTime)
  {
    if(m_type == txt)
      fprintf(m_fp, " +++++");
    else
      fprintf(m_fp, ",+++++");
  }
  else
  {
    double stat = double(test_size) / m_delta[test].Elapsed;
    if(test == Lseek)
    {
      if(m_type == txt)
      {
        if(stat >= 1000.0)
          fprintf(m_fp, " %5.0f", stat);
        else
          fprintf(m_fp, " %5.1f", stat);
      }
      else
      {
        if(stat >= 1000.0)
          fprintf(m_fp, ",%.0f", stat);
        else
          fprintf(m_fp, ",%.1f", stat);
      }
    }
    else
    {
      if(m_type == txt)
        fprintf(m_fp, " %5d", int(stat));
      else
        fprintf(m_fp, ",%d", int(stat));
    }
  }
  print_cpu_stat(test);
  return 0;
}

int BonTimer::print_latency(tests_t test)
{
  char buf[10];
  if(m_delta[test].Latency == 0.0)
  {
    buf[0] = '\0';
  }
  else
  {
    if(m_delta[test].Latency > 99.999999)
      _snprintf(buf
#ifndef NO_SNPRINTF
, sizeof(buf)
#endif
              , "%ds", int(m_delta[test].Latency));
    else if(m_delta[test].Latency > 0.099999)
      _snprintf(buf
#ifndef NO_SNPRINTF
, sizeof(buf)
#endif
              , "%dms", int(m_delta[test].Latency * 1000.0));
    else
      _snprintf(buf
#ifndef NO_SNPRINTF
, sizeof(buf)
#endif
              , "%dus", int(m_delta[test].Latency * 1000000.0));
  }
  if(m_type == txt)
  {
    fprintf(m_fp, " %9s", buf);
  }
  else
  {
    fprintf(m_fp, ",%s", buf);
  }
  return 0;
}

void
BonTimer::PrintHeader(FILE *fp)
{
  fprintf(fp, "format_version,bonnie_version,name,concurrency,seed,file_size,io_chunk_size,putc,putc_cpu,put_block,put_block_cpu,rewrite,rewrite_cpu,getc,getc_cpu,get_block,get_block_cpu,seeks,seeks_cpu");
  fprintf(fp, ",num_files,max_size,min_size,num_dirs,file_chunk_size,seq_create,seq_create_cpu,seq_stat,seq_stat_cpu,seq_del,seq_del_cpu,ran_create,ran_create_cpu,ran_stat,ran_stat_cpu,ran_del,ran_del_cpu");
  fprintf(fp, ",putc_latency,put_block_latency,rewrite_latency,getc_latency,get_block_latency,seeks_latency,seq_create_latency,seq_stat_latency,seq_del_latency,ran_create_latency,ran_stat_latency,ran_del_latency");
  fprintf(fp, "\n");
  fflush(NULL);
}

void print_size(char *buf, unsigned int size, CPCCHAR units)
{
  sprintf(buf, "%d", size);
  int ind = 0;
  if(units)
  {
    if(size == 0)
    {
      ind = strlen(buf);
      buf[ind] = units[0];
      buf[ind + 1] = '\0';
    }
    else
    {
      while(size % 1024 == 0 && units[ind + 1] != '\0')
      {
        size /= 1024;
        ind++;
      }
      sprintf(buf, "%d%c", size, units[ind]);
    }
  }
  ind = strlen(buf) - 1;
  if(buf[ind] == ' ')
    buf[ind] = '\0';
}

int
BonTimer::DoReportIO(int file_size, int char_file_size
                   , int io_chunk_size, FILE *fp)
{
  int i;
  m_fp = fp;
  const int txt_machine_size = 20;
  PCCHAR separator = ":";
  if(m_type == csv)
    separator = ",";
  if(file_size)
  {
    if(m_type == txt)
    {
      fprintf(m_fp, "Version %5s       ", BON_VERSION);
      fprintf(m_fp,
        "------Sequential Output------ --Sequential Input- --Random-\n");
      fprintf(m_fp, "Concurrency %3d     ", m_concurrency);
      fprintf(m_fp,
        "-Per Chr- --Block-- -Rewrite- -Per Chr- --Block-- --Seeks--\n");
      if(io_chunk_size == DefaultChunkSize)
        fprintf(m_fp, "Machine        Size ");
      else
        fprintf(m_fp, "Machine   Size:chnk ");
      fprintf(m_fp, "K/sec %%CP K/sec %%CP K/sec %%CP K/sec %%CP K/sec ");
      fprintf(m_fp, "%%CP  /sec %%CP\n");
    }
    char size_buf[1024];
    print_size(size_buf, file_size, "MG");
    char *tmp = size_buf + strlen(size_buf);
    if(io_chunk_size != DefaultChunkSize)
    {
      strcat(tmp, separator);
      tmp += strlen(tmp);
      print_size(tmp, io_chunk_size, " km");
    }
    else if(m_type == csv)
    {
      strcat(tmp, separator);
      tmp += strlen(tmp);
    }
    char buf[4096];
    if(m_type == txt)
    {
      // copy machine name to buf
      //
      _snprintf(buf
#ifndef NO_SNPRINTF
, txt_machine_size - 1
#endif
              , "%s                  ", m_name);
      buf[txt_machine_size - 1] = '\0';
      // set cur to point to a byte past where we end the machine name
      // size of the buf - size of the new data - 1 for the space - 1 for the
      // terminating zero on the string
      char *cur = &buf[txt_machine_size - strlen(size_buf) - 2];
      *cur = ' '; // make cur a space
      cur++; // increment to where we store the size
      strcpy(cur, size_buf);  // copy the size in
      fputs(buf, m_fp);
    }
    else
    {
      printf(CSV_VERSION "," BON_VERSION ",%s,%d,%s,%s", m_name
           , m_concurrency, random_source.getSeed().c_str(), size_buf);
    }
    for(i = ByteWrite; i < Lseek; i++)
    {
      if(i == ByteWrite || i == ByteRead)
        print_stat(tests_t(i), char_file_size * 1024);
      else
        print_stat(tests_t(i), file_size * 1024);
    }
    print_stat(Lseek, Seeks);
    if(m_type == txt)
    {
      fprintf(m_fp, "\nLatency          ");
      for(i = ByteWrite; i <= Lseek; i++)
        print_latency(tests_t(i));
      fprintf(m_fp, "\n");
    }
  }
  else if(m_type == csv)
  {
    fprintf(m_fp, CSV_VERSION "," BON_VERSION ",%s,%d,%s,,,,,,,,,,,,,,", m_name
          , m_concurrency, random_source.getSeed().c_str());
  }
  return 0;
}

int
BonTimer::DoReportFile(int directory_size
                     , int max_size, int min_size, int num_directories
                     , int file_chunk_size, FILE *fp)
{
  PCCHAR separator = ":";
  m_fp = fp;
  int i;
  if(m_type == csv)
    separator = ",";
  if(directory_size)
  {
    char buf[128];
    char *tmp;
    sprintf(buf, "%d", directory_size);
    tmp = &buf[strlen(buf)];
    if(m_type == csv)
    {
      if(max_size == -1)
      {
        sprintf(tmp, ",link,");
      }
      else if(max_size == -2)
      {
        sprintf(tmp, ",symlink,");
      }
      else if(max_size)
      {
        if(min_size)
          sprintf(tmp, ",%d,%d", max_size, min_size);
        else
          sprintf(tmp, ",%d,", max_size);
      }
      else
      {
        sprintf(tmp, ",,");
      }
      strcat(tmp, separator);
      tmp += strlen(tmp);
      if(file_chunk_size != DefaultChunkSize)
      {
        tmp++;
        print_size(tmp, file_chunk_size, " km");
      }
    }
    else
    {
      if(max_size == -1)
      {
        sprintf(tmp, ":link");
      }
      else if(max_size == -2)
      {
        sprintf(tmp, ":symlink");
      }
      else if(max_size)
      {
        sprintf(tmp, ":%d:%d", max_size, min_size);
      }
    }
    tmp = &buf[strlen(buf)];
    if(num_directories > 1)
    {
      if(m_type == txt)
        sprintf(tmp, "/%d", num_directories);
      else
        sprintf(tmp, ",%d", num_directories);
    }
    else if(m_type == csv)
    {
       sprintf(tmp, ",");
    }
    if(m_type == txt)
    {
      fprintf(m_fp, "Version %5s       ", BON_VERSION);
      fprintf(m_fp,
        "------Sequential Create------ --------Random Create--------\n");
      fprintf(m_fp, "%-19.19s ", m_name);
      fprintf(m_fp,
           "-Create-- --Read--- -Delete-- -Create-- --Read--- -Delete--\n");
      if(min_size)
      {
        fprintf(m_fp, "files:max:min       ");
      }
      else
      {
        if(max_size)
          fprintf(m_fp, "files:max           ");
        else
          fprintf(m_fp, "              files ");
      }
      fprintf(m_fp, " /sec %%CP  /sec %%CP  /sec %%CP  /sec %%CP  /sec ");
      fprintf(m_fp, "%%CP  /sec %%CP\n");
      fprintf(m_fp, "%19s", buf);
    }
    else
    {
      fprintf(m_fp, ",%s", buf);
    }
    for(i = CreateSeq; i < TestCount; i++)
      print_stat(tests_t(i), directory_size * DirectoryUnit);
    if(m_type == txt)
    {
      fprintf(m_fp, "\nLatency          ");
      for(i = CreateSeq; i < TestCount; i++)
        print_latency(tests_t(i));
    }
  }
  else if(m_type == csv)
  {
    fprintf(m_fp, ",,,,,,,,,,,,,,,,,");
  }
  if(m_type == csv)
  {
    for(i = ByteWrite; i < TestCount; i++)
      print_latency(tests_t(i));
  }
  fprintf(m_fp, "\n");
  fflush(NULL);
  return 0;
}

