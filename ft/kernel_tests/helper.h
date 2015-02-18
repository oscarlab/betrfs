#ifndef __HELPER__H__
#define __HELPFER_H__

// To use KERN_INFO
#define KERN_SON  "\001"      
#define KERN_ALERT KERN_SON "1"


#define DBG printf(KERN_ALERT "This is line %d of file %s (fun %s)\n", __LINE__, __FILE__, __func__)

#define STR(str)  printf(KERN_ALERT "%s\n", str)
#define INT(x)  printf(KERN_ALERT "%d\n", x)
#define LONG(x)  printf(KERN_ALERT "%ld\n", x)
#define ULONG(x)  printf(KERN_ALERT "%lu\n", x)

#endif 

