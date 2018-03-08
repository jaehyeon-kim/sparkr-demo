# Multiple plot function
#
# ggplot objects can be passed in ..., or to plotlist (as a list of ggplot objects)
# - cols:   Number of columns in layout
# - layout: A matrix specifying the layout. If present, 'cols' is ignored.
#
# If the layout is something like matrix(c(1,2,3,3), nrow=2, byrow=TRUE),
# then plot 1 will go in the upper left, 2 will go in the upper right, and
# 3 will go all the way across the bottom.
#
# source: http://www.cookbook-r.com/Graphs/Multiple_graphs_on_one_page_(ggplot2)/
#
multiplot <- function(..., plotlist=NULL, file, cols=1, layout=NULL) {
  library(grid)
  
  # Make a list from the ... arguments and plotlist
  plots <- c(list(...), plotlist)
  
  numPlots = length(plots)
  
  # If layout is NULL, then use 'cols' to determine layout
  if (is.null(layout)) {
    # Make the panel
    # ncol: Number of columns of plots
    # nrow: Number of rows needed, calculated from # of cols
    layout <- matrix(seq(1, cols * ceiling(numPlots/cols)),
                     ncol = cols, nrow = ceiling(numPlots/cols))
  }
  
  if (numPlots==1) {
    print(plots[[1]])
    
  } else {
    # Set up the page
    grid.newpage()
    pushViewport(viewport(layout = grid.layout(nrow(layout), ncol(layout))))
    
    # Make each plot, in the correct location
    for (i in 1:numPlots) {
      # Get the i,j matrix positions of the regions that contain this subplot
      matchidx <- as.data.frame(which(layout == i, arr.ind = TRUE))
      
      print(plots[[i]], vp = viewport(layout.pos.row = matchidx$row,
                                      layout.pos.col = matchidx$col))
    }
  }
}

summarise_cont <- function(dat, which) {
  dots <- list(mean = paste0('mean', '(', which, ')'),
               median = paste0('median', '(', which, ')'),
               min = paste0('min', '(', which, ')'),
               max = paste0('max', '(', which, ')'))
  tmp <- dat %>% dplyr::group_by(is_delay) %>%
    dplyr::summarise_(.dots = dots)
  bind_cols(data.frame(col=rep(which, times = nrow(tmp))), tmp)
}

summarise_cat <- function(dat, which) {
  dat %>% dplyr::group_by_(.dots = c(which, 'is_delay')) %>%
    dplyr::summarise(n = n()) %>%
    tidyr::spread(is_delay, n) %>%
    dplyr::mutate(count = yes+no, ratio = yes/no)  
}

get_multiplot <- function(which, cols = 2) {
  g1 <- ggplot(dat_s, aes_string(which)) + geom_bar(aes_string(fill='is_delay'))
  plot_lst <- list(g1)
  if(cols > 1) {
    g2 <- ggplot(summarise_cat(dat_s, which), aes_string(x=which, y='ratio')) +
      geom_bar(stat = 'identity', fill = 'steel blue')
    plot_lst <- append(plot_lst, list(g2))
  }
  multiplot(plotlist = plot_lst, cols = cols)
}

get_feat_importance <- function(model) {
  s <- summary(model)
  features <- s$features %>% unlist()
  imp_ext <- stringr::str_extract_all(s$featureImportances, '\\[(.*?)\\]') %>% 
    unlist()
  importance <- imp_ext[length(imp_ext)] %>% 
    stringr::str_replace_all('[\\[|\\]]', '') %>%
    strsplit(',') %>% unlist() %>% as.numeric()
  data.frame(feature = features, importance = importance) %>%
    dplyr::arrange(-importance)
}