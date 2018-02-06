FROM ubuntu:16.04
MAINTAINER Jaehyeon Kim <Jaehyeon.Kim@corelogic.com.au>

ARG R_VERSION
ENV R_VERSION=${R_VERSION:-3.4.3}

# http://jtremblay.github.io/software_installation/2017/06/21/Install-R-3.4.0-and-RStudio-on-Ubuntu-16.04
# https://github.com/rocker-org/rocker-versioned/blob/master/r-ver/Dockerfile
RUN apt-get update
RUN apt-get install -y \
        build-essential \
        fort77 \
        xorg-dev \
        liblzma-dev \
        libblas-dev \
        gfortran \
        gcc-multilib \
        gobjc++ \
        libreadline-dev \
        libcurl4-openssl-dev \
        texlive-latex-base \
        libcairo2-dev \
        libxml2-dev \
        libbz2-dev \
        zip \
        unzip \
        lpr \
        default-jdk

RUN apt-get install -y r-base

RUN curl -O https://cran.r-project.org/src/base/R-3/R-${R_VERSION}.tar.gz \
    && tar -xf R-${R_VERSION}.tar.gz \
    && cd R-${R_VERSION} \
    ## Set compiler flags
    && R_PAPERSIZE=letter \
        R_BATCHSAVE="--no-save --no-restore" \
        R_BROWSER=xdg-open \
        PAGER=/usr/bin/pager \
        PERL=/usr/bin/perl \
        R_UNZIPCMD=/usr/bin/unzip \
        R_ZIPCMD=/usr/bin/zip \
        R_PRINTCMD=/usr/bin/lpr \
        LIBnn=lib \
        AWK=/usr/bin/awk \
        CFLAGS="-g -O2 -fstack-protector-strong -Wformat -Werror=format-security -Wdate-time -D_FORTIFY_SOURCE=2 -g" \
        CXXFLAGS="-g -O2 -fstack-protector-strong -Wformat -Werror=format-security -Wdate-time -D_FORTIFY_SOURCE=2 -g" \
    ## Configure options
    ./configure --enable-R-shlib \
               --enable-memory-profiling \
               --with-readline \
               --with-blas \
               --with-tcltk \
               --disable-nls \
               --without-recommended-packages \
               --prefix=/usr/local/lib/R \
               --with-cairo=yes \
               --with-x=yes \
    ## Build and install
    && make \
    && make install \
    ## Add a default CRAN mirror
    && echo "options(repos = c(CRAN = 'https://cran.rstudio.com/'), download.file.method = 'libcurl')" >> /usr/local/lib/R/etc/Rprofile.site \
    ## Add a library directory (for user-installed packages)
    && mkdir -p /usr/local/lib/R/site-library \
    && chown root:staff /usr/local/lib/R/site-library \
    && chmod g+wx /usr/local/lib/R/site-library \
    ## Fix library path
    && echo "R_LIBS_USER='/usr/local/lib/R/site-library'" >> /usr/local/lib/R/etc/Renviron \
    && echo "R_LIBS=\${R_LIBS-'/usr/local/lib/R/site-library:/usr/local/lib/R/library:/usr/lib/R/library'}" >> /usr/local/lib/R/etc/Renviron \
    ## install packages from date-locked MRAN snapshot of CRAN
    && [ -z "$BUILD_DATE" ] && BUILD_DATE=$(TZ="America/Los_Angeles" date -I) || true \
    && MRAN=https://mran.microsoft.com/snapshot/${BUILD_DATE} \
    && echo MRAN=$MRAN >> /etc/environment \
    && export MRAN=$MRAN \
    ## MRAN becomes default only in versioned images
    ## Clean up from R source install
    && cd / \
    && rm -rf /tmp/* \
    && apt-get remove --purge -y $BUILDDEPS \
    && apt-get autoremove -y \
    && apt-get autoclean -y \
    && rm -rf /var/lib/apt/lists/*
