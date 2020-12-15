# Evenness calcs
library(rgdal)
library(tidyverse)


# read in spatial data ----

gdb_dir = 'C:/FTOT/scenarios/common_data/networks/Public_Intermodal_Network_2019_3.gdb'

# dir.exists(gdb_dir)
# ogrListLayers(gdb_dir)

# road = readOGR(gdb_dir, layer = 'road')
# save(list = 'road', file = 'RoadLayer.RData')
load('RoadLayer.RData')

# First, get IDs
road_ID = unlist(lapply(road@lines, function(x) x@ID))

road_dat = data.frame(road_ID, road@data)



# Read in this file edges ----

root_dir = 'C:/FTOT/scenarios/quick_start/Evenness_Calc_Tables'

# Calculate evenness ----

evenness_func <- function(d, measure = 'capac_minus_volume_zero_floor', bin_by = NULL, n_bin = 10){
  # bin_by = 'max_edge_capacity'
  # 
  if(is.null(bin_by)){
    S = nrow(d)
    Hmax = log(S)
    
    sum_weight = sum(d[,measure], na.rm = T)
    props = d[,measure] / sum_weight 
    Hprime = -1 * sum(log(props[!is.na(props)]) * props[!is.na(props)], na.rm=T)
    Evenness = Hprime / Hmax
  }
  if(!is.null(bin_by)){
    dx = d
    dx$bins = cut(dx[,bin_by], breaks = n_bin)
    
    S = n_bin
    Hmax = log(S)
    
    props = dx %>%
      ungroup() %>%
      mutate(sum_weight = sum(get(measure), na.rm=T)) %>%
      group_by(bins) %>%
      summarize(class_sum = sum(get(measure), na.rm=T),
                props = mean(class_sum  / sum_weight)) 
    
    Hprime = -1 * sum(log(props$props[!is.na(props$props)]) * props$props[!is.na(props$props)], na.rm=T)
    Evenness = Hprime / Hmax
    
  }
  Evenness
  
  
  
}

# Loop overall all scenarios ----

scens = c('MA', 'MA_2',
          'OH', 'OH_2',
          'CA', 'CA_2')
results = vector()

for(i in scens){
  #i = 'MA' # 'OH_2'
  
  d <- read.csv(file.path(root_dir, paste0('qs7_', i,'_edges.csv')))
  
  d = d %>%
    filter(mode == 'road')
  
  # Get functional class
  
  d$mode_oid = as.character(d$mode_oid)
  
  # join road attributes to these edges
  d <- d %>%
    left_join(road_dat, by = c('mode_oid' = 'road_ID')) %>%
    filter(!duplicated(mode_oid))
  
  # 1a. Available Capacity, zero floor
  e1a <-evenness_func(d, 'capac_minus_volume_zero_floor', bin_by = NULL)
  
  # 1b. Volume-capacity ratio
  e1b <- evenness_func(d, 'VCR', bin_by = NULL)
  
  # 2. Binned by capacity
  
  # 2a: Volume Capacity Ratio  
  
  e2a <- evenness_func(d, 'VCR', bin_by = 'max_edge_capacity')
  
  # 2b: Sum miles of roadway
  
  e2b <- evenness_func(d, 'MILES', bin_by = 'max_edge_capacity')
  
  res_i = data.frame(scen = i, e1a, e1b, e2a, e2b)
  
  results = rbind(results, res_i)
  
  cat(i, '\n\n')
  
}

write.csv(results, file = 'Evenness_Calcs.csv', row.names = F)

# Join with scenario cost outputs ----

scens = c('MA', 'MA_2',
          'OH', 'OH_2',
          'CA', 'CA_2')

scen_pct_ch <- vector()

for(i in scens){
  # i = 'MA'
  res = read.csv(file.path(root_dir, paste0('Results_', i,'.csv')))
  
  if(i == 'CA'){
    # fix cost calcs for california, should be in millions but got truncated
    res$total_cost[res$total_cost == '1,005'] = 1005704
    res$total_cost[res$total_cost == '1,028'] = 1028529
  }
  # clean up commas
  res$total_cost <- as.numeric(sub('\\,', '', res$total_cost))
  
  pct_ch = res %>%
    mutate(pct_ch = 100*(total_cost - res$total_cost[1])/res$total_cost,
           scen = i) %>%
    select(scen, disrupt_step, pct_ch)
  
  scen_pct_ch <- rbind(scen_pct_ch, pct_ch)
  
  # original version
  g_i <- ggplot(res, aes(x = disrupt_step,
                        y = total_cost)) +
    geom_step(size = 2, color = 'grey80') +
    geom_point(size = 2) + #, aes(color = nedge)) +
    theme_bw() +
    ggtitle(paste0(i)) +
    xlab('Disruption Step') + ylab('Total Scenario Cost')+ 
    labs(subtitle = paste0(' 2a. Evenness based on VCR: ', round(results[results$scen == i, 'e2a'], 3), '\n 2b. Evenness based on Miles: ', round(results[results$scen == i, 'e2b'], 3)))

  
  # same scale... not useful figure
  # g_i <- ggplot(res, aes(x = disrupt_step, 
  #                        y = total_cost)) +
  #   geom_step(size = 2, color = 'grey80') +
  #   ylim(180000, 1030000) +
  #   geom_point(size = 2) + #, aes(color = nedge)) +
  #   theme_bw() +
  #   ggtitle(paste0(i,' scenario \n Evenness based on VCR: ', round(results[results$scen == i, 'e2a'], 3), '\n',
  #                  ' Evenness based on Miles: ', round(results[results$scen == i, 'e2b'], 3)))
  
  assign(paste0('gplot_', i), g_i)
  
  
}

g_all <- egg::ggarrange(gplot_MA_2, gplot_OH_2,
               gplot_MA,   gplot_OH,
               gplot_CA_2, gplot_CA,
             nrow = 3, ncol = 2)

ggsave(plot = g_all,
       file = "Compiled_Evenness_Disruption_Figs.jpeg",
       width = 8, height = 10, dpi = 400, units = 'in')

# New plot: percent change over steps, for each 
scen_pct_ch <- left_join(scen_pct_ch, results, by = 'scen')

scen_pct_ch2 <- scen_pct_ch %>%
  mutate(scen_lab = NA) %>%
  group_by(scen) %>%
  mutate(scen_lab = ifelse(disrupt_step == 50, scen, NA))

ggplot(scen_pct_ch,
       aes(x = disrupt_step,
           y = pct_ch,
           color = e2a)) +
  geom_line() + 
  facet_wrap(~scen)


e2a <- ggplot(scen_pct_ch2,
              aes(x = disrupt_step,
                  y = pct_ch,
                  color = e2a,
                  group = scen)) +
  ylab('Percent change in  total cost') +
  xlab('Disruption step') +
  geom_line(size = 1.5, alpha = 0.8) +
  geom_text(aes(label = scen_lab), check_overlap = F,
            nudge_x = 2) +
  labs(color = 'Evenness: \n Capacity-binned, VCR') +
  theme_bw() +
  ggtitle('Percent change in total scenario cost across six scenarios \n by capacity-binned VCR Evenness')


e2b <- ggplot(scen_pct_ch2,
              aes(x = disrupt_step,
                  y = pct_ch,
                  color = e2b,
                  group = scen)) +
  ylab('Percent change in  total cost') +
  xlab('Disruption step') +
  geom_line(size = 1.5, alpha = 0.8) +
  geom_text(aes(label = scen_lab), check_overlap = F,
            nudge_x = 2) +
  labs(color = 'Evenness: \n Capacity-binned, Miles') +
  theme_bw() +
  ggtitle('Percent change in total scenario cost across six scenarios \n by Evenness 2b. Capacity-binned, Miles')


ggsave(plot = e2b, file = "One_Panel_Pct_Change_Evenness_Disruption_Fig.jpeg",
       width = 7, height = 6, dpi = 400, units = 'in')

## Another one-panel fig: maximum percent change

max_ch = scen_pct_ch %>%
  group_by(scen) %>%
  summarize(max_pct_ch = max(pct_ch))

results = left_join(results, max_ch, by = 'scen')

# Wide to long
res_long = results %>%
  pivot_longer(cols = c('e1a', 'e1b', 'e2a', 'e2b'))

res_long$name = as.factor(res_long$name)
levels(res_long$name) = c('1a. Link-level, Available Capacity',
                          '1b. Link-level, VCR',
                          '2a. Capacity-binned, VCR',
                          '2b. Capacity-binned, Miles')


ggplot(res_long, aes(x = max_pct_ch, y = value, color = scen)) +
  geom_point(size = 2) +
  facet_wrap(~name) +
  xlab('Maximum Percent Change in Total Scenario Cost') +
  ylab('Evenness Metric Value') +
  labs(color = 'Scenario') +
  theme_bw() +
  ggtitle('Four evenness metrics compared to maximum percent change \n in total scenario cost for six scenarios evaluated')


ggsave(file = "One_Panel_Max_Pct_Change_4Evenness_Fig.jpeg",
       width = 7, height = 6, dpi = 400, units = 'in')
