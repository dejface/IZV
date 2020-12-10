#!/usr/bin/env python3.8
# coding=utf-8

"""
File: analysis.py
Author: David Oravec (xorave05)
"""

from matplotlib import pyplot as plt
import pandas as pd
import seaborn as sns
import numpy as np
import os, gzip, pickle
# muzete pridat libovolnou zakladni knihovnu ci knihovnu predstavenou na prednaskach
# dalsi knihovny pak na dotaz


# Ukol 1: nacteni dat
def get_dataframe(filename: str, verbose: bool = False) -> pd.DataFrame:

    string_list = [
        "p1",               # #int identifikacne cislo
        "p36",              # #int druh pozemnej komunikacie
        "p37",              # #int cislo pozemnej komunikacie
        "weekday(p2a)",     # #int den v tyzdni
        "p2b",              # #int cas
        "p6",               # #int druh nehody
        "p7",               # #int druh zrazky vozidiel
        "p8",               # #int druh pevnej prekazky
        "p9",               # #int charakter nehody
        "p10",              # #int zavinenie nehody
        "p11",              # #int alkohol u vinnika
        "p12",              # #int hlavna pricina nehody
        "p13a",             # #int mrtve osoby
        "p13b",             # #int tazko zranene osoby
        "p13c",             # #int lahko zranene osoby
        "p14",              # #int celkova hmotna skoda
        "p15",              # #int povrch vozovky
        "p16",              # #int stav povrchu vozovky v dobe nehody
        "p17",              # #int stav komunikacie
        "p18",              # #int poveternostne podmienky v dobe nehody
        "p19",              # #int viditelnost
        "p20",              # #int rozhladove pomery
        "p21",              # #int delenie komunikacie
        "p22",              # #int situovanie nehody na komunikacii
        "p23",              # #int riadenie premavky v dobe nehody
        "p24",              # #int miestna uprava prednosti v jazde
        "p27",              # #int specificke miesta a objekty na mieste nehody
        "p28",              # #int smerove pomery
        "p34",              # #int pocet zucastnenych vozidiel
        "p35",              # #int miesto dopravnej nehody
        "p39",              # #int druh krizujucej komunikacie
        "p44",              # #int druh vozidla
        "p45a",             # #int vyrobna znacka motoroveho vozidla
        "p47",              # #int rok vyroby vozidla
        "p48a",             # #int charakteristika vozidla
        "p49",              # #int smyk
        "p50a",             # #int vozidlo po nehode
        "p50b",             # #int unik prepravovanych hmot
        "p51",              # #int sposob vybratia osob z vozidla
        "p52",              # #int smer jazdy alebo postavenie vozidla
        "p53",              # #int skoda na vozidle
        "p55a",             # #int kategoria vodica
        "p57",              # #int stav vodica
        "p58",              # #int vonkajsie ovplyvnenie vodica
        "a",                # #float
        "b",                # #float
        "d",                # #float suradnice x
        "e",                # # float suradnice y
        "f",                # #float
        "g",                # #float
        "h",                # #str
        "i",                # #str
        "j",                # #str
        "k",                # #str
        "l",                # #str
        "n",                # #int
        "o",                # #str
        "p",                # #str P a O si nie som isty
        "q",                # #str
        "r",                # #int
        "s",                # #int
        "t",                # #str
        "p5a",              # #int
        "region"]           # #str skratka regionu

    df = pd.read_pickle(os.path.join(os.path.dirname
                        (os.path.abspath(__file__)), filename),
                        compression='gzip')
    orig_size = round(df.memory_usage(index=True, deep=True).sum() / 1048576, 1)

    for _, i in enumerate(string_list):
        if i == 'region':
            break
        elif i == 'p13a' or i == 'p13b' or i == 'p13c' or i == 'p53':
            df[i] = pd.to_numeric(df[i])
        else:
            df[i] = pd.Categorical(df[i])

    df['p2a'] = pd.to_datetime(df['p2a'])
    df.rename(columns={'p2a': 'date'}, inplace=True)

    new_size = round(df.memory_usage(index=True, deep=True).sum() / 1048576, 1)
    if (verbose):
        print(f"orig_size={orig_size} MB")
        print(f"new_size={new_size} MB")

    return df


# Ukol 2: následky nehod v jednotlivých regionech
def plot_conseq(df: pd.DataFrame, fig_location: str = None,
                show_figure: bool = False):

    pd.melt(df, id_vars=['region'], value_vars=['p13a', 'p13b', 'p13c'])
    deaths_per_region = df.groupby('region', as_index=False)\
        .agg({'p13a': 'sum'}).reset_index().sort_values('p13a', ascending=False)
    heavy_injuries = df.groupby('region', as_index=False)\
        .agg({'p13b': 'sum'}).reset_index().sort_values('p13b', ascending=False)
    light_injuries = df.groupby('region', as_index=False)\
        .agg({'p13c': 'sum'}).reset_index().sort_values('p13c', ascending=False)
    accidents_count = df.groupby('region', as_index=False)\
        .agg({'p1': 'count'}).reset_index().sort_values('p1', ascending=False)

    fig, (ax1, ax2, ax3, ax4) = plt.subplots(4, 1, figsize=(11.69, 8.27), sharex=True)
    fig.suptitle("Number of accidents in Czech regions through years 2016-2020",
                 fontsize=12)
    fig.subplots_adjust(hspace=0.5, top=0.91, bottom=0.05)
    fig.patch.set_facecolor('#C0C0C0')

    # deaths plot
    graph1 = sns.barplot(data=deaths_per_region, y=deaths_per_region['p13a'],
                         x=accidents_count['region'], ax=ax1, color='#CC00CC')
    graph1.spines['right'].set_visible(False)
    graph1.spines['top'].set_visible(False)
    graph1.set_title('Deaths', fontsize=8)
    graph1.set(ylabel='Count', xlabel=None, yticks=[100, 200, 300, 400])
    graph1.grid(color='#95a5a6', linestyle='-', linewidth=1, axis='y',
                alpha=0.3)
    graph1.set_facecolor('#FFCCFF')
    count = 0
    for _, row in accidents_count.iterrows():
        for _, inner_row in deaths_per_region.iterrows():
            if inner_row.name == row.name:
                graph1.text(count, 2, inner_row.p13a, color='black', ha="center", fontsize=8)
                count += 1
                break

    # heavy injuries plot
    graph2 = sns.barplot(data=heavy_injuries, y=heavy_injuries['p13b'],
                         x=accidents_count['region'], ax=ax2, color='#3399FF')
    graph2.set_title('Heavy injuries', fontsize=8)
    graph2.set(ylabel='Count', xlabel=None, yticks=[400, 800, 1200, 1600])
    graph2.spines['right'].set_visible(False)
    graph2.spines['top'].set_visible(False)
    graph2.grid(color='#95a5a6', linestyle='-', linewidth=1, axis='y',
                alpha=0.3)
    graph2.set_facecolor('#CCCCFF')
    count = 0
    for _, row in accidents_count.iterrows():
        for _, inner_row in heavy_injuries.iterrows():
            if inner_row.name == row.name:
                graph2.text(count, 0, inner_row.p13b, color='black', ha="center", fontsize=8)
                count += 1
                break

    # light injuries plot
    graph3 = sns.barplot(data=light_injuries, y=light_injuries['p13c'],
                         x=accidents_count['region'], ax=ax3, color='#006633')
    graph3.set_title('Light injuries', fontsize=8)
    graph3.set(ylabel='Count', xlabel=None, yticks=[3500, 7000, 10500, 14000])
    graph3.spines['right'].set_visible(False)
    graph3.spines['top'].set_visible(False)
    graph3.grid(color='#95a5a6', linestyle='-', linewidth=1, axis='y',
                alpha=0.3)
    graph3.set_facecolor('#CCFFE5')
    count = 0
    for _, row in accidents_count.iterrows():
        for _, inner_row in light_injuries.iterrows():
            if inner_row.name == row.name:
                graph3.text(count, 0, inner_row.p13c, color='black', ha="center", fontsize=8)
                count += 1
                break

    # total accidents plot
    graph4 = sns.barplot(data=accidents_count, y=accidents_count['p1'],
                         x=accidents_count['region'], ax=ax4, color='#FF0000')
    graph4.set_title('Total accidents', fontsize=8)
    graph4.set(ylabel='Count', xlabel=None, yticks=[25000, 50000, 75000, 100000])
    graph4.spines['right'].set_visible(False)
    graph4.spines['top'].set_visible(False)
    graph4.grid(color='#95a5a6', linestyle='-', linewidth=1, axis='y',
                alpha=0.3)
    graph4.set_facecolor('#FFCCCC')
    count = 0
    for _, row in accidents_count.iterrows():
        graph4.text(count, 0, row.p1, color='black', ha="center", fontsize=8)
        count += 1

    if show_figure:
        plt.show()

    if fig_location is not None:
        fig.savefig(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                    fig_location), dpi=600)


# Ukol3: příčina nehody a škoda
def plot_damage(df: pd.DataFrame, fig_location: str = None,
                show_figure: bool = False):

    pha_df = df[df['region'] == 'PHA'].copy()
    stc_df = df[df['region'] == 'STC'].copy()
    jhm_df = df[df['region'] == 'JHM'].copy()
    lbk_df = df[df['region'] == 'LBK'].copy()
    new_df = pd.concat([pha_df, stc_df, jhm_df, lbk_df], sort=False)

    new_df['Damage cause'] = pd.cut(x=new_df['p12'],
                                    bins=[100, 201, 301, 401, 501, 601, 615],
                                    right=False,
                                    include_lowest=True,
                                    labels=['wasn\'t caused by driver',
                                            'excessive speed',
                                            'incorrect overtaking',
                                            'wasn\'t given priority in driving',
                                            'incorrect way of driving',
                                            'technical problem on a vehicle'])

    new_df['damage'] = pd.cut(x=new_df['p53'],
                              bins=[0, 500, 2000, 5000, 10000, np.inf],
                              right=False,
                              include_lowest=True,
                              labels=['<50', '50 - 200', '200 - 500',
                                      '500 - 1000', '>1000'])

    new_df['count'] = 0
    df_final = new_df.groupby(['region', 'damage', 'Damage cause'],
                              as_index=False)['p1'].agg(['count']).reset_index()

    graph1 = sns.catplot(x='damage', col='region', y='count', data=df_final,
                         kind='bar', hue='Damage cause',
                         col_wrap=2, ci=None, sharex=False)
    graph1.set_axis_labels("Damage [thousands CZK]", "Count")
    graph1.fig.get_axes()[0].set_yscale('log')
    graph1.fig.suptitle('Causes of accidents and their damage [thousands CZK] '
                        'through years 2016-2020', fontsize=11)
    graph1.fig.subplots_adjust(hspace=0.25, top=0.91, bottom=0.15)
    graph1.fig.patch.set_facecolor('#C0C0C0')
    axes = graph1.axes.flatten()
    axes[0].set_title('JHM', fontsize=9)
    axes[1].set_title('LBK', fontsize=9)
    axes[2].set_title('PHA', fontsize=9)
    axes[3].set_title('STC', fontsize=9)
    for i in range(4):
        axes[i].spines['right'].set_visible(False)
        axes[i].spines['top'].set_visible(False)
        axes[i].grid(color='#95a5a6', linestyle='-', linewidth=1, axis='y',
                     alpha=0.3)
        axes[i].set_facecolor('#808080')

    if show_figure:
        plt.show()

    if fig_location is not None:
        graph1.fig.savefig(os.path.join(os.path.dirname
                           (os.path.abspath(__file__)), fig_location), dpi=600)


# Ukol 4: povrch vozovky
def plot_surface(df: pd.DataFrame, fig_location: str = None,
                 show_figure: bool = False):

    pha_df = df[df['region'] == 'PHA'].copy()
    stc_df = df[df['region'] == 'STC'].copy()
    jhm_df = df[df['region'] == 'JHM'].copy()
    lbk_df = df[df['region'] == 'LBK'].copy()
    new_df = pd.concat([pha_df, stc_df, jhm_df, lbk_df], sort=False)

    cont_df = pd.crosstab(index=[new_df['region'], new_df['date']],
                          columns=new_df['p16'])
    cont_df = cont_df.rename(columns={0: 'other state',
                             1: 'dry surface - non polluted',
                             2: 'dry surface - polluted (sand,dust,leaves,gravel,...)',
                             3: 'wet surface',
                             4: 'surface with mud',
                             5: 'surface with ice, snow - sprinkled',
                             6: 'surface with ice, snow - non sprinkled',
                             7: 'surface with spilled oil, gas,...',
                             8: 'contiguous snow layer',
                             9: 'sudden change of road surface (ice, frost on a bridge,...)'})

    cont_df = cont_df.stack(level=['p16'])
    cont_df = cont_df.to_frame().reset_index()

    final = cont_df.groupby(['region', 'p16',
                            pd.Grouper(freq='M', key='date')]).sum().reset_index()
    final = final.rename(columns={0: 'count',
                         'p16': 'State of road surface'})

    for i in final:
        if i == 'region':
            final[i] = final[i].astype(str)
        elif i == 'State of road surface':
            final[i] = pd.Categorical(final[i])
        elif i == 'date':
            final[i] = pd.to_datetime(final[i])
        elif i == 'count':
            final[i] = pd.to_numeric(final[i])

    graph1 = sns.relplot(data=final, x="date", y="count",
                         hue="State of road surface", col="region", kind="line",
                         col_wrap=2, height=3.8)
    graph1.set_axis_labels("Date of accident", "Number of accidents")
    graph1.fig.suptitle('State of road surface by accidents ' +
                        'through years 2016-2020', fontsize=11)
    graph1.fig.subplots_adjust(hspace=0.25, top=0.91, bottom=0.15)
    graph1.fig.patch.set_facecolor('#C0C0C0')
    axes = graph1.axes.flatten()
    axes[0].set_title('JHM', fontsize=9)
    axes[1].set_title('LBK', fontsize=9)
    axes[2].set_title('PHA', fontsize=9)
    axes[3].set_title('STC', fontsize=9)
    for i in range(4):
        axes[i].grid(color='#95a5a6', linestyle='-', linewidth=1, axis='y',
                     alpha=0.3)
        axes[i].set_facecolor('#808080')

    if show_figure:
        plt.show()

    if fig_location is not None:
        graph1.fig.savefig(os.path.join(os.path.dirname
                           (os.path.abspath(__file__)), fig_location), dpi=600)


if __name__ == "__main__":
    pass
    # zde je ukazka pouziti, tuto cast muzete modifikovat podle libosti
    # skript nebude pri testovani pousten primo, ale budou volany konkreni ¨
    # funkce.
    df = get_dataframe("accidents.pkl.gz", True)
    plot_conseq(df, fig_location="01_nasledky.png")
    plot_damage(df, "02_priciny.png")
    plot_surface(df, "03_stav.png")
