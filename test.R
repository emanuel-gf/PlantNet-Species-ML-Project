R.home("bin")
library(ggplot2)
library(dplyr)
library(tidyverse)

##
# Load
dataf <- readr::read_csv("tmp_csv/model_results.csv", col_names=TRUE)

metrics_col <- c("recall","precision","f1","accuracy","balanced_accuracy","roc_auc")

datafilter <- dataf |> 
                    filter(metric != "confusion_matrix") |>
                    mutate(value = as.numeric(value))
# plot
ggplot(datafilter |> filter(metric != "kfold"),
            aes(x = model, y = value, fill = metric)
        ) +
  stat_summary(fun = mean, geom = "bar", position = "dodge") +
  stat_summary(fun.data = mean_se, geom = "errorbar", 
               #position = position_dodge(width = 0.9),
                width = 0.05, color='#444343ff') +
  labs(title = "Models Performance",
       x =NULL, y = "Score") +
    scale_fill_brewer(palette = "Blues")+
    theme_minimal() +
    theme(axis.text.x = element_text(angle = 45, hjust = 1),
        plot.background = element_rect(fill = "#eaf2f8", color = NA)
        ) 

ggsave("result_imgs/my_plot.png",    
       width = 8,           
       height = 6,          
       dpi = 300)  
    