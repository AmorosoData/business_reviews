'''
business_review.py
A program to power Epiphany's Business Reviews

    Usage:
        business_review.py [--customer CUSTOMER]
                           [--date DATE]

    Options:
        -h --help                                   Show this message and exit
        --customer=CUSTOMER                         Name of the customer to run the file on
        --date=DATE                                 Date to start the report run

    Example:
        python3 business_review.py --customer="customer1" --date="20201012"
'''

import os
import sys
import pandas as pd
import datetime
import pymysql
from docopt import docopt
import paramiko
from paramiko import AuthenticationException
from paramiko.ssh_exception import NoValidConnectionsError

"""
Potential Future Upgrades
1. Read from previous Business Review files and append new month results to it
    a. This would keep the number of files being written to only 1 per customer
        1. Overwrite existing file after reading in?
        2. Delete existing file after reading in?
2. When customer is not specified, append all results to 1 file
    a. Would need to add a customer_name column to the Business Review Final DataFrame
        1. File Export Names
            a. 2022_12_Business_Reviews or
            b. 2022_Dec_Business_Reviews
    b. Steps to complete
        1. Create Final DataFrame first with columns
        2. Before final customer results are appended to the Final DataFrame, create customer_name column from name in customers dict
        3. Append results to Final DataFrame
        4. Move to next customer
"""
"""
To Do List
Redemptions
    - Filter out NULL trxid
"""
# Working Path to tmp folder
# On Mac, create a tmp folder in your main directory then change below file path
# This is used to put the completed excel files and if STFP is enabled, will pick file from here to transfer to SFTP
# _WORKING_PATH = "/tmp/"

# For Testing on Local Machine
_WORKING_PATH = "/Users/namoroso/Projects/reporting/monthly_business_review/"

"""
To add a new customer
1. Copy below:
    {
        "name": "",
        "portal_name": "",
        "parent_co": "",
        "include_locations_group": [],
        "exclude_locations_group": []
    },

2. Add in the name of the customer
3. Add in the portal name (coalition customers are kickback)
4. Add in a parent company id (Mostly for coalition customers)
5. Add in ppccustomers.custid to include in the report go in include_locations_group
6. Add in ppccustomers.custid to exclude from the report go in exclude_locations_group
7. Place customer in alphabetical order in the customers list of dicts
"""

# Customers List for Business Reviews
customers = [
    {
        "name": "acacia",
        "portal_name": "acacia",
        "parent_co": "",
        "include_locations_group": [],
        "exclude_locations_group": [2,3,7,8,9,10]
    },
    {
       "name": "advanced_land_holdings",
       "portal_name": "advancedlandholdings",
       "parent_co": "",
       "include_locations_group": [],
       "exclude_locations_group": []
    },
    {   
       "name": "airport_plaza",
       "portal_name": "airportplaza",
       "parent_co": "",
       "include_locations_group": [],
       "exclude_locations_group": []
    },
    {
        "name": "alamosa",
        "portal_name": "kickback",
        "parent_co": "875",
        "include_locations_group": [73456],
        "exclude_locations_group": [],
    },
    {
        "name": "aloha",
        "portal_name": "aloha",
        "parent_co": "",
        "include_locations_group": [],
        "exclude_locations_group": [1,102,104]
    },
    {
        "name": "alon",
        "portal_name": "alon",
        "parent_co": "",
        "include_locations_group": [],
        "exclude_locations_group": [],
    },
    {   
        "name": "alta_convenience",#pester
        "portal_name": "kickback",
        "parent_co": "410",
        "include_locations_group": [],
        "exclude_locations_group": [],
    },
    {   
        "name": "amerimart",#expressmart
        "portal_name": "kickback",
        "parent_co": "547",
        "include_locations_group": [],
        "exclude_locations_group": [],
    },
    {
        "name": "arrowhead",
        "portal_name": "arrowhead",
        "parent_co": "",
        "include_locations_group": [],
        "exclude_locations_group": [],
    },
    {   
        "name": "autopia",
        "portal_name": "autopia",
        "parent_co": "",
        "include_locations_group": [],
        "exclude_locations_group": [],
    },
    {
        "name": "alta_convenience",
        "portal_name": "kickback",
        "parent_co": "410",
        "include_locations_group": [],
        "exclude_locations_group": [72482, 72483, 72488, 72506, 72507, 72924, 74482, 74483, 74626, 74627, 74628, 74629, 74630, 74751, 74770, 74776, 74783, 74785, 75234, 75580, 75581, 75582, 75583, 75584],
    },
    {
        "name": "american_royal_petroleum",
        "portal_name": "kickback",
        "parent_co": "628",
        "include_locations_group": [],
        "exclude_locations_group": [],
    },
    {
        "name": "asap_general",
        "portal_name": "kickback",
        "parent_co": "565",
        "include_locations_group": [],
        "exclude_locations_group": [72978,73425,72722,75420,73602,73602,73492,73492,72978,73492,72704,72706,72710,72974,73492,73493,73496,74427,74715],
    },
    {
        "name": "asap_lucilles_roadhouse",
        "portal_name": "kickback",
        "parent_co": "565",
        "include_locations_group": [72722, 72974],
        "exclude_locations_group": [],
    },
    {
        "name": "asap_roadhouse_clinton",
        "portal_name": "kickback",
        "parent_co": "565",
        "include_locations_group": [75420],
        "exclude_locations_group": [],
    },
    {
        "name": "asap_ricks_boots",
        "portal_name": "kickback",
        "parent_co": "565",
        "include_locations_group": [74715],
        "exclude_locations_group": [],
    },
    {
       "name": "asap_non_trendar",
       "portal_name": "kickback",
       "parent_co": "565",
       "include_locations_group": [72705,72706,72710,73492,73493,73494,73495,73496,73497,73897,74427],
       "exclude_locations_group": []
    },
    {
       "name": "asap_trendar",
       "portal_name": "kickback",
       "parent_co": "565",
       "include_locations_group": [72704,72707,72708,72709,73036,73498,74472,74592,75049],
       "exclude_locations_group": []
    },
    {
       "name": "awss",
       "portal_name": "kickback",
       "parent_co": "801",
       "include_locations_group": [],
       "exclude_locations_group": []
    },
    {
        "name": "bigapplerewards",
        "portal_name": "bigapplerewards",
        "parent_co": "",
        "include_locations_group": [],
        "exclude_locations_group": [76, 50, 71, 75]
    },
    {
        "name": "biindigen",
        "portal_name": "biindigen",
        "parent_co": "",
        "include_locations_group": [1],
        "exclude_locations_group": [],
    },
    {
        "name": "bishop_paiute",
        "portal_name": "bishop_paiute",
        "parent_co": "",
        "include_locations_group": [],
        "exclude_locations_group": [],
    },
    {   
        "name": "bobby_and_steves_world_rewards",
        "portal_name": "bobbyandsteves",
        "parent_co": "",
        "include_locations_group": [],
        "exclude_locations_group": [],
    },
    {   
        "name": "bonneaubonus",
        "portal_name": "bonneaubonus",
        "parent_co": "",
        "include_locations_group": [],
        "exclude_locations_group": [4, 5, 6],
    },
    {   
        "name": "bread_and_butter",
        "portal_name": "breadandbutter",
        "parent_co": "",
        "include_locations_group": [],
        "exclude_locations_group": [],
    },
    {   
        "name": "burke_rewards",
        "portal_name": "burkerewards",
        "parent_co": "",
        "include_locations_group": [],
        "exclude_locations_group": [],
    },
    {
        "name": "canyon_ferry_big_sky",
        "portal_name": "canyonferry",
        "parent_co": "",
        "include_locations_group": [],
        "exclude_locations_group": [3,4]
    },
    {
        "name": "bells",
        "portal_name": "kickback",
        "parent_co": "301",
        "include_locations_group": [],
        "exclude_locations_group": [],
    },
    {
        "name": "big_o_oil",
        "portal_name": "kickback",
        "parent_co": "809",
        "include_locations_group": [],
        "exclude_locations_group": [],
    },
    {
        "name": "big_oil_and_tire",
        "portal_name": "kickback",
        "parent_co": "1211",
        "include_locations_group": [],
        "exclude_locations_group": [],
    },
    {
        "name": "blairs_truckstop",
        "portal_name": "kickback",
        "parent_co": "503",
        "include_locations_group": [],
        "exclude_locations_group": [],
    },
    {
		"name": "brians",
        "portal_name": "kickback",
        "parent_co": "491",
        "include_locations_group": [],
        "exclude_locations_group": [],
    },
    {
        "name": "chandler_enterprises",
        "portal_name": "kickback",
        "parent_co": "646",
        "include_locations_group": [],
        "exclude_locations_group": []
    },
    {
        "name": "civic_energy_center",
        "portal_name": "kickback",
        "parent_co": "775",
        "include_locations_group": [],
        "exclude_locations_group": []
    },
    {
        "name": "c_barn",
        "portal_name": "kickback",
        "parent_co": "1162",
        "include_locations_group": [],
        "exclude_locations_group": [],
    },
    {
        "name": "coleman_oil",
        "portal_name": "kickback",
        "parent_co": "486",
        "include_locations_group": [],
        "exclude_locations_group": [],
    },
    {
        "name": "conoco_homeland",
        "portal_name": "kickback",
        "parent_co": "1300",
        "include_locations_group": [],
        "exclude_locations_group": [75320, 75322],
    },
    {
        "name": "conrad_bischoff",
        "portal_name": "kickback",
        "parent_co": "",
        "include_locations_group": [71219],
        "exclude_locations_group": [],
    },
    {
        "name": "corrigan_my_rewards",
        "portal_name": "corriganmyrewards",
        "parent_co": "",
        "include_locations_group": [],
        "exclude_locations_group": [3, 5],
    },
    {
        "name": "crater_lake_travel_center_klamath",
        "portal_name": "klamath",
        "parent_co": "",
        "include_locations_group": [],
        "exclude_locations_group": []
    },
    {
        "name": "damascus_76",
        "portal_name": "kickback",
        "parent_co": "1094",
        "include_locations_group": [],
        "exclude_locations_group": []
    },
    {
        "name": "divine_corp",
        "portal_name": "kickback",
        "parent_co": "647",
        "include_locations_group": [],
        "exclude_locations_group": []
    },
    {
        "name": "e_and_c_mid_atlantic_corner_mart",
        "portal_name": "kickback",
        "parent_co": "977",
        "include_locations_group": [74138,74254,74255,74256,74257,74258,74259,74260,74261],
        "exclude_locations_group": []
    },
    {
       "name": "dons_car_wash",
       "portal_name": "kickback",
       "parent_co": "1116",
       "include_locations_group": [],
       "exclude_locations_group": []
    },
    {
       "name": "double_r_liquors",
       "portal_name": "kickback",
       "parent_co": "738",
       "include_locations_group": [],
       "exclude_locations_group": []
    },
    {   
       "name": "dar_usa",
       "portal_name": "darusa",
       "parent_co": "",
       "include_locations_group": [],
       "exclude_locations_group": []
    },
    {
        "name": "e_and_c_mid_atlantic_non_corner_mart",
        "portal_name": "kickback",
        "parent_co": "977",
        "include_locations_group": [74131,74132,74133,74134,74136,74137,74139,74140,74671,74673,74674,74675,74676,74677],
        "exclude_locations_group": []
    },
    {
        "name": "edward_marzel",
        "portal_name": "kickback",
        "parent_co": "415",
        "include_locations_group": [],
        "exclude_locations_group": []
    },
    {
        "name": "expressmart_home_service_oil",
        "portal_name": "expressmart",
        "parent_co": "",
        "include_locations_group": [],
        "exclude_locations_group": []
    },
    {
        "name": "extramile",
        "portal_name": "extramile",
        "parent_co": "",
        "include_locations_group": [],
        "exclude_locations_group": [1,2,894,895,1012]
    },
    {
        "name": "fab_freddys",
        "portal_name": "freddy",
        "parent_co": "",
        "include_locations_group": [],
        "exclude_locations_group": [7, 12, 17, 18, 19, 20, 21, 22, 23, 24, 25]
    },
    {
        "name": "fab_freddys_no_lube",
        "portal_name": "freddy",
        "parent_co": "",
        "include_locations_group": [1, 2, 3, 4, 5, 6, 7, 8, 13, 26],
        "exclude_locations_group": []
    },
    {
        "name": "fab_freddys_lube",
        "portal_name": "freddy",
        "parent_co": "",
        "include_locations_group": [9,10,11,14],
        "exclude_locations_group": []
    },
    {
        "name": "fast_break_ed_staub",
        "portal_name": "fastbreak",
        "parent_co": "",
        "include_locations_group": [],
        "exclude_locations_group": []
    },
    {
        "name": "fast_n_friendly",
        "portal_name": "kickback",
        "parent_co": "545",
        "include_locations_group": [],
        "exclude_locations_group": []
    },
    {
        "name": "fill_n_chill",
        "portal_name": "uncle_neals",
        "parent_co": "",
        "include_locations_group": [],
        "exclude_locations_group": []
    },
    {
        "name": "fisher_fuel_market",
        "portal_name": "fuelmarket",
        "parent_co": "",
        "include_locations_group": [],
        "exclude_locations_group": [14]
    },
    {
        "name": "fleetway_rewards",
        "portal_name": "fleetwayrewards",
        "parent_co": "",
        "include_locations_group": [],
        "exclude_locations_group": []
    },
    {
        "name": "get_n_go_olson",
        "portal_name": "getngo",
        "parent_co": "",
        "include_locations_group": [],
        "exclude_locations_group": []
    },
    {
        "name": "greeneville",
        "portal_name": "greenevillequickstop",
        "parent_co": "",
        "include_locations_group": [],
        "exclude_locations_group": [35,34]
    }, 
    {
       "name": "frank_griffin_oil_company",
       "portal_name": "kickback",
       "parent_co": "614",
       "include_locations_group": [],
       "exclude_locations_group": []
    },
    {
       "name": "franken_oil_n_distributing_com",
       "portal_name": "kickback",
       "parent_co": "674",
       "include_locations_group": [],
       "exclude_locations_group": []
    },
    {
        "name": "gmreward",
        "portal_name": "gmreward",
        "parent_co": "",
        "include_locations_group": [],
        "exclude_locations_group": [15, 16]
    },
    {
        "name": "gs_services",
        "portal_name": "kickback",
        "parent_co": "514",
        "include_locations_group": [],
        "exclude_locations_group": []
    },
    {
        "name": "gm_cono",
        "portal_name": "kickback",
        "parent_co": "795",
        "include_locations_group": [],
        "exclude_locations_group": []
    },
    {
        "name": "hammer_williams", #aka Jiffy Trip
        "portal_name": "kickback",
        "parent_co": "1317",
        "include_locations_group": [],
        "exclude_locations_group": [75659]
    },
    {
        "name": "handymart",
        "portal_name": "handymart",
        "parent_co": "",
        "include_locations_group": [],
        "exclude_locations_group": []
    },
    {
        "name": "harts",
        "portal_name": "harts",
        "parent_co": "",
        "include_locations_group": [],
        "exclude_locations_group": [4,5,10]
    },
    {
        "name": "hattenhauer_distributing",
        "portal_name": "kickback",
        "parent_co": "651",
        "include_locations_group": [],
        "exclude_locations_group": []
    },
    {
        "name": "highland_corp_fast_stop_perks",
        "portal_name": "faststopperks",
        "parent_co": "",
        "include_locations_group": [],
        "exclude_locations_group": [17]
    },
    {
        "name": "hutchinson_oil",
        "portal_name": "kickback",
        "parent_co": "1338",
        "include_locations_group": [],
        "exclude_locations_group": []
    },
    {
        "name": "jacksons",
        "portal_name": "jacksons",
        "parent_co": "",
        "include_locations_group": [],
        "exclude_locations_group": []
    },
    {
        "name": "jimdandy",
        "portal_name": "jimdandy",
        "parent_co": "",
        "include_locations_group": [],
        "exclude_locations_group": [3,9]
    },
    {
        "name": "jrs_country_store",
        "portal_name": "kickback",
        "parent_co": "412",
        "include_locations_group": [],
        "exclude_locations_group": []
    },
    {
        "name": "kb_oil",
        "portal_name": "kboil",
        "parent_co": "",
        "include_locations_group": [],
        "exclude_locations_group": [1, 9, 11, 12, 13]
    },
    {
        "name": "leerjak_ross_oil",
        "portal_name": "leerjak",
        "parent_co": "",
        "include_locations_group": [],
        "exclude_locations_group": []
    },
    {
        "name": "lennys_rt_66_food_and_fuel",
        "portal_name": "rt66foodandfuel",
        "parent_co": "",
        "include_locations_group": [],
        "exclude_locations_group": []
    },
    {
        "name": "ltbb_biindigen",
        "portal_name": "biindigen",
        "parent_co": "",
        "include_locations_group": [],
        "exclude_locations_group": []
    },
    {
        "name": "lynch_oil_mr_gas",
        "portal_name": "mrgas",
        "parent_co": "",
        "include_locations_group": [],
        "exclude_locations_group": [2, 4, 9, 10, 12, 13, 15, 16, 17]
    },
    {
        "name": "maccardrewards",
        "portal_name": "maccardrewards",
        "parent_co": "",
        "include_locations_group": [],
        "exclude_locations_group": [4]
    },
    {
        "name": "meyer_oil_c_stores",
        "portal_name": "meyeroilcompany",
        "parent_co": "",
        "include_locations_group": [],
        "exclude_locations_group": [15, 16, 17, 25, 28]
    },
    {
        "name": "meyer_oil_liquor_stores",
        "portal_name": "meyeroilcompany",
        "parent_co": "",
        "include_locations_group": [15, 16],
        "exclude_locations_group": []
    },
    {
        "name": "millers",
        "portal_name": "millers",
        "parent_co": "",
        "include_locations_group": [],
        "exclude_locations_group": [17, 18, 26]
    },
    {
        "name": "minitstop",
        "portal_name": "minitstop",
        "parent_co": "",
        "include_locations_group": [],
        "exclude_locations_group": []
    },
    {
        "name": "missouri_valley_petroleum",
        "portal_name": "kickback",
        "parent_co": "295",
        "include_locations_group": [],
        "exclude_locations_group": []
    },
    {
        "name": "mitchem_enterprise",
        "portal_name": "kickback",
        "parent_co": "977",
        "include_locations_group": [],
        "exclude_locations_group": []
    },
    {
        "name": "mvp_bulk_fuels",
        "portal_name": "kickback",
        "parent_co": "969",
        "include_locations_group": [74040],
        "exclude_locations_group": []
    },
    {
        "name": "new_distributing",
        "portal_name": "kickback",
        "parent_co": "590",
        "include_locations_group": [],
        "exclude_locations_group": []
    },
    {
        "name": "north_stratford_minimart",
        "portal_name": "kickback",
        "parent_co": "490",
        "include_locations_group": [],
        "exclude_locations_group": []
    },
    {
        "name": "oasis_stop_n_go",
        "portal_name": "kickback",
        "parent_co": "321",
        "include_locations_group": [],
        "exclude_locations_group": [72024,71373,71374,71375,70873,70872,70870,70869,70865,71146,71147,71144,71145,71142,71101,72148,74100,74713,75557,75558,75559,71141,71320,71321,72095,71322]
    },
    {
        "name": "oasis_stop_n_go_travelers",
        "portal_name": "kickback",
        "parent_co": "321",
        "include_locations_group": [70873,74713],
        "exclude_locations_group": []
    },
    {
        "name": "obos_market_and_deli",
        "portal_name": "obo",
        "parent_co": "",
        "include_locations_group": [],
        "exclude_locations_group": []
    },
    {
        "name": "oncue_no_16_17",
        "portal_name": "kickback",
        "parent_co": "1219",
        "include_locations_group": [],
        "exclude_locations_group": [74724,74723,75114,75473,75343]
    },
    {
        "name": "oncue_16_17",
        "portal_name": "kickback",
        "parent_co": "1219",
        "include_locations_group": [74724,74723],
        "exclude_locations_group": []
    },
    {
        "name": "orton_oil",
        "portal_name": "ortons",
        "parent_co": "",
        "include_locations_group": [],
        "exclude_locations_group": []
    },
    {
        "name": "par_hawaii_76",
        "portal_name": "kickback",
        "parent_co": "387",
        "include_locations_group": [],
        "exclude_locations_group": [75813, 75393, 72178, 72179, 72250, 72169, 74693, 74311, 74307, 72156, 72166, 72175, 74821, 74838, 74352, 74353, 74354]
    },
    {
        "name": "par_hawaii_hele",
        "portal_name": "kickback",
        "parent_co": "1347",
        "include_locations_group": [],
        "exclude_locations_group": [75678, 75754]
    },
    {
        "name": "pdq",
        "portal_name": "kickback",
        "parent_co": "614",
        "include_locations_group": [],
        "exclude_locations_group": []
    },
    {
        "name": "pester",
        "portal_name": "kickback",
        "parent_co": "412",
        "include_locations_group": [],
        "exclude_locations_group": []
    },
    {
        "name": "peak_rewards",#dba Denali Express
        "portal_name": "peakrewards",
        "parent_co": "",
        "include_locations_group": [],
        "exclude_locations_group": []
    },
    {
        "name": "petes_of_erie",
        "portal_name": "kickback",
        "parent_co": "556",
        "include_locations_group": [],
        "exclude_locations_group": [72696,75778]
    },
    {
        "name": "qrewards",
        "portal_name": "qrewards",
        "parent_co": "",
        "include_locations_group": [],
        "exclude_locations_group": [21,2, 22, 25, 28, 30, 34, 36, 37, 41, 42, 43, 45, 46, 48, 50, 51, 52]
    },
    {
        "name": "quality_dairy",
        "portal_name": "qualitydairy",
        "parent_co": "",
        "include_locations_group": [],
        "exclude_locations_group": [21,27]
    },
    {
        "name": "quality_dairy_expired",
        "portal_name": "qualitydairy",
        "parent_co": "",
        "include_locations_group": [27],
        "exclude_locations_group": []
    },
    {
        "name": "rapid_roberts",
        "portal_name": "kickback",
        "parent_co": "393",
        "include_locations_group": [],
        "exclude_locations_group": [72249]
    },
    {
        "name": "red_carpet",
        "portal_name": "kickback",
        "parent_co": "1267",
        "include_locations_group": [],
        "exclude_locations_group": []
    },
    {
        "name": "reed_inc_r_place",
        "portal_name": "kickback",
        "parent_co": "291",
        "include_locations_group": [70841,70842,70844,70840],
        "exclude_locations_group": []
    },
    {
        "name": "roseville_station",
        "portal_name": "kickback",
        "parent_co": "409",
        "include_locations_group": [],
        "exclude_locations_group": []
    },
    {
        "name": "schrader_oil",
        "portal_name": "schraderoil",
        "parent_co": "",
        "include_locations_group": [],
        "exclude_locations_group": []
    }, 
    {   
       "name": "7_stars_rewards",
       "portal_name": "sevenstars",
       "parent_co": "",
       "include_locations_group": [],
       "exclude_locations_group": []
    },
    {   
       "name": "signals",
       "portal_name": "signals",
       "parent_co": "",
       "include_locations_group": [],
       "exclude_locations_group": []
    },
    {   
       "name": "silver_reef_casino",
       "portal_name": "silverreefcasino",
       "parent_co": "",
       "include_locations_group": [],
       "exclude_locations_group": []
    },
    {   
       "name": "silver_reef",
       "portal_name": "silver_reef",
       "parent_co": "",
       "include_locations_group": [],
       "exclude_locations_group": []
    },
    {   
       "name": "spirit_rewards",
       "portal_name": "spiritrewards",
       "parent_co": "",
       "include_locations_group": [],
       "exclude_locations_group": []
    },
    {   
       "name": "sprint_mart_rewards",
       "portal_name": "sprintmart",
       "parent_co": "",
       "include_locations_group": [],
       "exclude_locations_group": []
    },
    {   
       "name": "stop_n_go_rewards",
       "portal_name": "stopngo",
       "parent_co": "",
       "include_locations_group": [],
       "exclude_locations_group": []
    },
    {   
       "name": "sullys",
       "portal_name": "sullys",
       "parent_co": "",
       "include_locations_group": [],
       "exclude_locations_group": [9]
    },
    {   
       "name": "swansons_rewards",
       "portal_name": "swansons",
       "parent_co": "",
       "include_locations_group": [],
       "exclude_locations_group": []
    },
    {   
       "name": "swinomish",
       "portal_name": "swinomish",
       "parent_co": "",
       "include_locations_group": [],
       "exclude_locations_group": []
    },
    {
       "name": "s_n_z_petroleum",
       "portal_name": "kickback",
       "parent_co": "748",
       "include_locations_group": [],
       "exclude_locations_group": []
    },
    {
       "name": "sand_canyon_service_station",
       "portal_name": "kickback",
       "parent_co": "851",
       "include_locations_group": [],
       "exclude_locations_group": []
    },
    {
       "name": "satnam_petroleum",
       "portal_name": "kickback",
       "parent_co": "778",
       "include_locations_group": [],
       "exclude_locations_group": []
    },
    {
       "name": "scdp_land_n_investment",
       "portal_name": "kickback",
       "parent_co": "891",
       "include_locations_group": [],
       "exclude_locations_group": []
    },
    {
       "name": "7_stars_rewards",
       "portal_name": "sevenstars",
       "parent_co": "",
       "include_locations_group": [],
       "exclude_locations_group": []
    },
    {   
       "name": "signals",
       "portal_name": "signals",
       "parent_co": "",
       "include_locations_group": [],
       "exclude_locations_group": []
    },
    {   
       "name": "silver_reef_casino",
       "portal_name": "silverreefcasino",
       "parent_co": "",
       "include_locations_group": [],
       "exclude_locations_group": []
    },
    {   
       "name": "silver_reef",
       "portal_name": "silver_reef",
       "parent_co": "",
       "include_locations_group": [],
       "exclude_locations_group": []
    },
    {   
       "name": "spirit_rewards",
       "portal_name": "spiritrewards",
       "parent_co": "",
       "include_locations_group": [],
       "exclude_locations_group": []
    },
    {   
       "name": "sprint_mart_rewards",
       "portal_name": "sprintmart",
       "parent_co": "",
       "include_locations_group": [],
       "exclude_locations_group": []
    },
    {   
       "name": "stop_n_go_rewards",
       "portal_name": "stopngo",
       "parent_co": "",
       "include_locations_group": [],
       "exclude_locations_group": []
    },
    {   
       "name": "sullys_oh_yes_rewards",
       "portal_name": "sullys",
       "parent_co": "",
       "include_locations_group": [],
       "exclude_locations_group": []
    },
    {   
       "name": "swansons_rewards",
       "portal_name": "swansons",
       "parent_co": "",
       "include_locations_group": [],
       "exclude_locations_group": []
    },
    {   
       "name": "swinomish",
       "portal_name": "swinomish",
       "parent_co": "",
       "include_locations_group": [],
       "exclude_locations_group": []
    },
    {
       "name": "s_n_z_petroleum",
       "portal_name": "kickback",
       "parent_co": "748",
       "include_locations_group": [],
       "exclude_locations_group": []
    },
    {
       "name": "sand_canyon_service_station",
       "portal_name": "kickback",
       "parent_co": "851",
       "include_locations_group": [],
       "exclude_locations_group": []
    },
    {
       "name": "satnam_petroleum",
       "portal_name": "kickback",
       "parent_co": "778",
       "include_locations_group": [],
       "exclude_locations_group": []
    },
    {
       "name": "scdp_land_n_investment",
       "portal_name": "kickback",
       "parent_co": "891",
       "include_locations_group": [],
       "exclude_locations_group": []
    },
    {
        "name": "sfc_fredrickson_76",
        "portal_name": "kickback",
        "parent_co": "610",
        "include_locations_group": [],
        "exclude_locations_group": []
    },
    {
        "name": "sierra_fuel",
        "portal_name": "kickback",
        "parent_co": "1234",
        "include_locations_group": [],
        "exclude_locations_group": []
    },
    {
        "name": "spoonville_ventures_food_n_fun",
        "portal_name": "foodnfun",
        "parent_co": "",
        "include_locations_group": [],
        "exclude_locations_group": []
    },
    {
        "name": "sprintmart",
        "portal_name": "sprintmart",
        "parent_co": "",
        "include_locations_group": [],
        "exclude_locations_group": [89, 94]
    },
    {
        "name": "stop_n_shop",
        "portal_name": "kickback",
        "parent_co": "527",
        "include_locations_group": [],
        "exclude_locations_group": []
    },
    {
        "name": "sullivan_petroleum_sullys",
        "portal_name": "sullys",
        "parent_co": "",
        "include_locations_group": [],
        "exclude_locations_group": [9]
    },
    {
        "name": "super_pumper",
        "portal_name": "kickback",
        "parent_co": "1297",
        "include_locations_group": [],
        "exclude_locations_group": []
    },
    {
        "name": "sutey_oil_thriftway_super_store",
        "portal_name": "kickback",
        "parent_co": "498",
        "include_locations_group": [],
        "exclude_locations_group": []
    },
    {
        "name": "terrible_herbst",
        "portal_name": "terribleherbst",
        "parent_co": "",
        "include_locations_group": [],
        "exclude_locations_group": []
    },
    {
        "name": "tommys_properties",
        "portal_name": "kickback",
        "parent_co": "867",
        "include_locations_group": [],
        "exclude_locations_group": [73460]
    },
    {
        "name": "trading_post_rewards",
        "portal_name": "tradingpostrewards",
        "parent_co": "",
        "include_locations_group": [],
        "exclude_locations_group": []
    },
    {
        "name": "viking_and_victory_enterprises",
        "portal_name": "kickback",
        "parent_co": "1121",
        "include_locations_group": [],
        "exclude_locations_group": []
    },
    {
        "name": "village_vendor",
        "portal_name": "kickback",
        "parent_co": "411",
        "include_locations_group": [],
        "exclude_locations_group": []
    },
    {
        "name": "warrenton_oil",
        "portal_name": "kickback",
        "parent_co": "703",
        "include_locations_group": [],
        "exclude_locations_group": []
    },
    {
        "name": "warrenton_oil_c_stores",
        "portal_name": "kickback",
        "parent_co": "703",
        "include_locations_group": [],
        "exclude_locations_group": [73045,73072,75818,75093,75085,72767]
    },
    {
        "name": "warrenton_oil_truck_stops",
        "portal_name": "kickback",
        "parent_co": "703",
        "include_locations_group": [73045,73072,75818,75093,75085],
        "exclude_locations_group": []
    },
    {
        "name": "whitehead_oil_u_stop",
        "portal_name": "kickback",
        "parent_co": "1149",
        "include_locations_group": [],
        "exclude_locations_group": [75616]
    },
    {
        "name": "woodsheds",
        "portal_name": "woodsheds",
        "parent_co": "",
        "include_locations_group": [],
        "exclude_locations_group": []
    },
    {
        "name": "ziggys_gas_and_grub",
        "portal_name": "kickback",
        "parent_co": "333",
        "include_locations_group": [],
        "exclude_locations_group": []
    },
    {   
       "name": "z_stop",
       "portal_name": "zstop",
       "parent_co": "",
       "include_locations_group": [],
       "exclude_locations_group": []
    }, 
]


def convert_trxdatetime_to_datetime(row):
    """ Converts Column to Date Time, this avoids Pandas trying to set a value on a copy error """
    if not row['transaction_datetime'] == '':
        return pd.to_datetime(row['transaction_datetime'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
    elif row['transaction_datetime'] == '':
        return pd.NaT
    else:
        return pd.NaT


def convert_activation_to_datetime(row):
    """ Converts Column to Date Time, this avoids Pandas trying to set a value on a copy error """
    if not row['activation_datetime'] == '':
        return pd.to_datetime(row['activation_datetime'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
    elif row['activation_datetime'] == '':
        return pd.NaT
    else:
        return pd.NaT


def convert_cancellation_to_datetime(row):
    """ Converts Column to Date Time, this avoids Pandas trying to set a value on a copy error """
    if not row['cancellation_datetime'] == '':
        return pd.to_datetime(row['cancellation_datetime'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
    elif row['cancellation_datetime'] == '':
        return pd.NaT
    else:
        return pd.NaT


def convert_lastpurchase_to_datetime(row):
    """ Converts Column to Date Time, this avoids Pandas trying to set a value on a copy error """
    if not row['last_purchase_datetime'] == '':
        return pd.to_datetime(row['last_purchase_datetime'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
    elif row['last_purchase_datetime'] == '':
        return pd.NaT
    else:
        return pd.NaT


def convert_personalinfo_to_datetime(row):
    """ Converts Column to Date Time, this avoids Pandas trying to set a value on a copy error """
    if not row['personal_info_modified_date'] == '':
        return pd.to_datetime(row['personal_info_modified_date'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
    elif row['personal_info_modified_date'] == '':
        return pd.NaT
    else:
        return pd.NaT


def convert_birthdate_to_date(row):
    """ Converts Column to Date Time, this avoids Pandas trying to set a value on a copy error """
    if not row['birth_date'] == '':
        return pd.to_datetime(row['birth_date'], format='%Y-%m-%d', errors='coerce')
    elif row['birth_date'] == '':
        return pd.NaT
    else:
        return pd.NaT


def convert_firsttrxdate_to_date(row):
    """ Converts Column to Date Time, this avoids Pandas trying to set a value on a copy error """
    if not row['first_trx_date'] == '':
        return pd.to_datetime(row['first_trx_date'], format='%Y-%m-%d', errors='coerce')
    elif row['first_trx_date'] == '':
        return pd.NaT
    else:
        return pd.NaT


def get_config():
    """
    Configuration Settings
    """
    try:
        loy_user = os.environ['LOYALTY_USER']
        loy_password = os.environ['LOYALTY_PWD']
        portal_host = os.environ['PORTAL_HOST']
        portal_db = os.environ['PORTAL_DB']
        sftp_host = os.environ['BRICKFTP_KRS_HOST']
        sftp_user = os.environ['EPIPHANY_SFTP_USER']
        sftp_password = os.environ['EPIPHANY_SFTP_PASSWORD']
    except KeyError:
        print('Missing Environmental Variable')
        sys.exit(9)

    config = {
        'loy_user': loy_user,
        'loy_password': loy_password,
        'portal_host': portal_host,
        'portal_db': portal_db,
        'sftp_host': sftp_host,
        'sftp_user': sftp_user,
        'sftp_password': sftp_password
    }
    return config


def sftp_connect(host, user, password):
    ''' Make ssh/sftp connections and return sftp object '''
    print("Creating SSH client")
    try:
        paramiko.sftp_file.SFTPFile.MAX_REQUEST_SIZE = 1024
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        print("Attempting SSH connect via username and password")
        client.connect(hostname=host,
                       username=user,
                       password=password,
                       allow_agent=False,
                       look_for_keys=False)
    except (NoValidConnectionsError, AuthenticationException) as exc:
        print("SFTP Authentication failed!")
        return None
    except Exception as exc:
        print(f"Error: {exc}")
        return None
    print("Creating SFTP client object from SSH connection object")
    try:
        sftp = client.open_sftp()
    except paramiko.sftp.SFTPError as exc:
        print("This error generally means one of the bash scripts (like .bashrc) is echo'ing something to stdout.  You should remove any echo statements.")
        return None
    except Exception as exc:
        print(f"Unable to connect due to {exc}")
        return None
    sftp.customer_host = host
    sftp.customer_user = user
    return sftp


def sftp_put_file(sftp_client, source_path, source_name, sftp_path):
    """ Copy file from local to remote sftp site """
    try:
        source = source_path + source_name
        destination = sftp_path + '/' + source_name
        print(f"Calling sftp_client.put({source}, {destination})")
        sftp_client.put(source, destination)
    except FileNotFoundError:
        print(f"Source file {source} not found.")
        return False
    except PermissionError as exc:
        print(f"Unable to place file due to {exc}")
        return False
    return True


def main():
    business_review_start = datetime.datetime.now()
    print("***** Starting Business Review Process *****")
    
    # Get Configuration Information
    conf = get_config()

    # Loyalty User and Password
    loy_user = conf['loy_user']
    loy_password = conf['loy_password']

    # Portal Host and Database
    portal_host = conf['portal_host']
    portal_db = conf['portal_db']

    # SFTP Information
    sftp_host = conf['sftp_host']
    sftp_destination_path = "Epiphany/Business Review"
    sftp_user = conf['sftp_user']
    sftp_password = conf['sftp_password']

    # Docopt Arguments
    args = docopt(__doc__, version="1.0")
    customer = args.get('--customer')
    date = args.get('--date')

    # Converts date to YYYY-MM-DD format
    new_date = str(date[0:4]) + '-' + str(date[4:6]) + '-' + str(date[6:8])
    # Converts new_date to an actual date
    new_date_conv = datetime.datetime.strptime(new_date, "%Y-%m-%d").date()
    # Grabs the last day of the month, when script is ran on the first of the month
    end_date = str(new_date_conv - datetime.timedelta(days=1))
    # Removes the dashes in end_date to make it YYYYMMDD format
    enddate = str(end_date).replace('-','')
    # Subtracts 14 days from end of last month, then changes the day to be the first
    start_date = str((new_date_conv - datetime.timedelta(days=14)).replace(day=1))
    # Removes the dashes in start_date to make it YYYYMMDD format
    startdate = str(start_date).replace('-','')

    # Portal Process
    portal_conn = f"mysql+pymysql://{loy_user}:{loy_password}@{portal_host}/{portal_db}"

    # Portal Connection Query
    portal_query = """
        SELECT
            name,
            db_read_url
        FROM
            db
    """
    portal_info = pd.read_sql(portal_query, con=portal_conn)

    # Prints the Start Date and End Dates of the reports
    # Helps to confirm that its running the correct dates
    print(f"Business Review Dates\nStart Date: {start_date}\nEnd Date: {end_date}")

    # Loops through every customer for the business review process
    for i in customers:
        customer_start = datetime.datetime.now()
        customer_name = i['name']
        customer_portal_name = [i['portal_name']]
        parent_co = i['parent_co']
		#include_locations_group = i['include_locations_group']
        include_locations_group = i.get('include_locations_group', [])
		#exclude_locations_group = i['exclude_locations_group']
        exclude_locations_group = i.get('exclude_locations_group', [])



        # If customer exists then proceeds with Business Review Process
        if customer == customer_name or customer == "" or customer is None:
            new_portal_info = portal_info[portal_info['name'].isin(customer_portal_name)].reset_index()
            new_portal_info = new_portal_info.drop(columns=['index'], axis=1)
            loyalty_host = new_portal_info.at[0, 'db_read_url']
            loyalty_db = new_portal_info.at[0, 'name']

            # Loyalty Connection String for MYSQL
			#loy_conn = f"mysql+pymysql://{loy_user}:{loy_password}@{loyalty_host}/{loyalty_db}"
            loy_conn = f"mysql+pymysql://{conf['loy_user']}:{conf['loy_password']}@{loyalty_host}/{loyalty_db}"
            print(f"Starting Business Review for {customer_name}")
            # Select the correct location query to use with parent company ids or not
            locations_start = datetime.datetime.now()
            if not parent_co == "":
                locations_query = """
                    SELECT
                        p.custid AS location_id,
                        p.name AS location_name
                    FROM ppccustomers p
                    WHERE p.iparentcoid = {}
                    """.format(parent_co)
            else:
                locations_query = """
                    SELECT
                        p.custid AS location_id,
                        p.name AS location_name
                    FROM ppccustomers p
                """         
            locations = pd.read_sql(locations_query, con=loy_conn)
            locations_end = datetime.datetime.now()
            locations_total_seconds = str((locations_end - locations_start).total_seconds())

            # Skip Customer if no location information is received
            if locations.empty:
                print(f"Unable to grab location information for {customer_name}.\nUnable to proceed with business review.")
                continue

            if not locations.empty:
                # Exclude specific locations
                # Locations like test locations, corporate, etc
                if not len(exclude_locations_group) == 0:
                    loy_locations = locations[~locations['location_id'].isin(exclude_locations_group)]
                elif not len(include_locations_group) == 0:
                    loy_locations = locations[locations['location_id'].isin(include_locations_group)]
                else:
                    loy_locations = locations.copy()

                # Create MYSQL LocationID IN clause
                # Convert Location ID Column to SQL Format ('1', '2', '3')
                location_ids = ""
                for loc in loy_locations.location_id:
                    if location_ids != "":
                        location_ids += ","
                    location_ids += "'{}'".format(loc)

                # Transaction Information
                transactions_start = datetime.datetime.now()
                transactions_query = """
                    SELECT
                        CardNumber AS cardnumber,
                        LocationID AS location_id,
                        TrxNumber AS transaction_number,
                        shiftnumber AS cashier_id,
                        PromoID AS promo_code,
                        TrxDate AS transaction_date,
                        TrxTime AS transaction_time,
                        Quantity AS transaction_total_sales,
                        Points AS transaction_points,
                        InvoiceDate AS transaction_invoice_date,
                        gallons AS transaction_gallons
                    FROM dailypointstrx
                    WHERE locationid IN ({}) AND
                    trxdate BETWEEN '{}' AND '{}'
                """.format(location_ids, startdate, enddate)
                trxs = pd.read_sql(transactions_query, con=loy_conn)
                transactions_end = datetime.datetime.now()
                transactions_total_seconds = str((transactions_end - transactions_start).total_seconds())

                if trxs.empty:
                    print(f"{customer_name} transactions df is empty.\nUnable to proceed with Business Review\nSkipping to Next Customer")
                    continue

                if not trxs.empty:
                    trxs.loc[:,'constant'] = 1
                    trxs['location_id'] = trxs['location_id'].astype(int)
                    trxs['transaction_points'] = trxs['transaction_points'].str.replace(" ", "0")
                    #trxs['transaction_points'] = trxs['transaction_points'].astype(float)
                    trxs['transaction_points'] = pd.to_numeric(trxs['transaction_points'], errors='coerce')
                    trxs['transaction_points'] = pd.to_numeric(trxs['transaction_points'], errors='coerce').fillna(0)
                    trxs['transaction_points'] = trxs['transaction_points'].astype(int)
                    trxs['trx_date'] = trxs['transaction_date'].str.slice(start=0, stop=4) + '-' + trxs['transaction_date'].str.slice(start=4, stop=6) + '-' + trxs['transaction_date'].str.slice(start=6, stop=8)
                    trxs['trx_time'] = trxs['transaction_time'].str.slice(start=0, stop=2) + ":" + trxs['transaction_time'].str.slice(start=2, stop=4) + ":00"
                    trxs['transaction_datetime'] = trxs['trx_date'] + ' ' + trxs['trx_time']
                    trxs['transaction_datetime'] = trxs['transaction_datetime'].replace('0000-00-00 00:00:00', '')
                    trxs['transaction_datetime'] = trxs.apply(convert_trxdatetime_to_datetime, axis=1)

                    # Split transactions df into 3 separate dfs
                    # 1 - non-bonus point transactions
                    # 2 - bonus point transactions
                    # 3 - Transaction Numbers de-duped
                    trxs_nobp = trxs.loc[~trxs['promo_code'].str.startswith("Pr")]
                    trxs_bp = trxs.loc[trxs['promo_code'].str.startswith("Pr")]
                    trx_numbers = trxs[['transaction_number', 'cardnumber', 'location_id', 'transaction_total_sales']].drop_duplicates(keep='first')

                    if not trxs_nobp.empty:
                        # Parent Level Transaction Groupings
                        parent_trxs = trxs_nobp.groupby(['constant']).agg(
                            total_transactions = ('transaction_number', pd.Series.nunique),
                            unique_customers = ('cardnumber', pd.Series.nunique),
                            total_nonbp_points_issued = ('transaction_points', 'sum'),
                            total_dollars = ('transaction_total_sales', 'sum'),
                            total_gallons = ('transaction_gallons', 'sum')
                        ).reset_index()
                        parent_trxs['constant'] = parent_trxs['constant'].astype(int)

                        # Site Level Transaction Groupings
                        site_trxs = trxs_nobp.groupby(['location_id']).agg(
                            total_transactions = ('transaction_number', pd.Series.nunique),
                            unique_customers = ('cardnumber', pd.Series.nunique),
                            total_nonbp_points_issued = ('transaction_points', 'sum'),
                            total_dollars = ('transaction_total_sales', 'sum'),
                            total_gallons = ('transaction_gallons', 'sum')
                        ).reset_index()
                        site_trxs['location_id'] = site_trxs['location_id'].astype(int)
                    else:
                        parent_trxs = pd.DataFrame()
                        parent_trxs['constant'] = 1
                        parent_trxs['total_transactions'] = 0
                        parent_trxs['unique_customers'] = 0
                        parent_trxs['total_nonbp_points_issued'] = 0
                        parent_trxs['total_dollars'] = 0.00
                        parent_trxs['total_gallons'] = 0.000

                        site_trxs = pd.DataFrame()
                        site_trxs['location_id'] = loy_locations['location_id']
                        site_trxs['total_transactions'] = 0
                        site_trxs['unique_customers'] = 0
                        site_trxs['total_nonbp_points_issued'] = 0
                        site_trxs['total_dollars'] = 0.00
                        site_trxs['total_gallons'] = 0.000

                    if not trxs_bp.empty:
                        # Parent Level Bonus Point Transaction Groupings
                        parent_trxs_bp = trxs_bp.groupby(['constant']).agg(
                            total_bp_transactions = ('transaction_number', pd.Series.nunique),
                            unique_bp_customers = ('cardnumber', pd.Series.nunique),
                            total_bp_points_issued = ('transaction_points', 'sum')
                        ).reset_index()
                        parent_trxs_bp['constant'] = parent_trxs_bp['constant'].astype(int)

                        # Site Level Bonus Point Transaction Groupings
                        site_trxs_bp = trxs_bp.groupby(['location_id']).agg(
                            total_bp_transactions = ('transaction_number', pd.Series.nunique),
                            unique_bp_customers = ('cardnumber', pd.Series.nunique),
                            total_bp_points_issued = ('transaction_points', 'sum')
                        ).reset_index()
                        site_trxs_bp['location_id'] = site_trxs_bp['location_id'].astype(int)
                    else:
                        parent_trxs_bp = pd.DataFrame()
                        parent_trxs_bp['constant'] = 1
                        parent_trxs_bp['total_bp_transactions'] = 0
                        parent_trxs_bp['unique_bp_customers'] = 0
                        parent_trxs_bp['total_bp_points_issued'] = 0

                        site_trxs_bp = pd.DataFrame()
                        site_trxs_bp['location_id'] = loy_locations['location_id']
                        site_trxs_bp['total_bp_transactions'] = 0
                        site_trxs_bp['unique_bp_customers'] = 0
                        site_trxs_bp['total_bp_points_issued'] = 0

                    # Fuel Transactions
                    gallons_limit = [0]
                    fuel_staging = trxs_nobp[~trxs_nobp['transaction_gallons'].isin(gallons_limit)]
                    fuel_trx_numbers_staging = fuel_staging[['transaction_number', 'transaction_gallons']].drop_duplicates(keep='first')

                    # Get Unique Fuel Transaction IDs with Total Fuel Gallons
                    fuel_trx_numbers = fuel_trx_numbers_staging.groupby(['transaction_number']).agg(
                        total_fuel_gallons = ('transaction_gallons', 'sum')
                    ).reset_index()

                    if not fuel_staging.empty:
                        # Parent Level Fuel Grouping
                        parent_fuel_trxs = fuel_staging.groupby(['constant']).agg(
                            unique_fuel_customers = ('cardnumber', pd.Series.nunique),
                            total_fuel_transactions = ('transaction_number', pd.Series.nunique)
                        ).reset_index()
                        parent_fuel_trxs['constant'] = parent_fuel_trxs['constant'].astype(int)

                        # Site Level Fuel Grouping
                        site_fuel_trxs = fuel_staging.groupby(['location_id']).agg(
                            unique_fuel_customers = ('cardnumber', pd.Series.nunique),
                            total_fuel_transactions = ('transaction_number', pd.Series.nunique)
                        ).reset_index()
                        site_fuel_trxs['location_id'] = site_fuel_trxs['location_id'].astype(int)
                    else:
                        parent_fuel_trxs = pd.DataFrame()
                        parent_fuel_trxs['constant'] = 0
                        parent_fuel_trxs['unique_fuel_customers'] = 0
                        parent_fuel_trxs['total_fuel_transactions'] = 0

                        site_fuel_trxs = pd.DataFrame()
                        site_fuel_trxs['location_id'] = loy_locations['location_id']
                        site_fuel_trxs['unique_fuel_customers'] = 0
                        site_fuel_trxs['total_fuel_transactions'] = 0

                    # Lineitem Transaction Information
                    lineitems_start = datetime.datetime.now()
                    lineitems_query = """
                        SELECT
                            d.LocationID AS location_id,
                            d.CardNumber AS cardnumber,
                            d.TrxNumber AS transaction_number,
                            d.TrxDate AS transaction_date,
                            d.PromoID AS promo_code,
                            li.codetype AS product_code_type,
                            li.productcodec AS product_code,
                            li.category AS product_category,
                            li.quantity AS product_quantity,
                            li.priceeach AS price_each,
                            li.priceeachregular AS price_each_regular,
                            li.total AS product_total_price,
                            li.points AS product_points,
                            li.linenumber AS line_number
                        FROM dailypointstrx d
                        INNER JOIN lineitems li ON li.trxid = d.trxnumber
                        WHERE
                            d.TrxDate BETWEEN '{}' AND '{}' AND
                            d.LocationID IN ({})
                    """.format(startdate, enddate, location_ids)
                    lineitems_trxs = pd.read_sql(lineitems_query, con=loy_conn)

                    if lineitems_trxs.empty:
                        parent_line_trxs = pd.DataFrame()
                        parent_line_trxs['constant'] = 1
                        parent_line_trxs['inside_transactions'] = 0
                        parent_line_trxs['inside_sales'] = 0.00

                        site_line_trxs = pd.DataFrame()
                        site_line_trxs['location_id'] = loy_locations['location_id']
                        site_line_trxs['inside_transactions'] = 0
                        site_line_trxs['inside_sales'] = 0.00

                    if not lineitems_trxs.empty:
                        lineitems_trxs.loc[:,'constant'] = 1
                        lineitems_trxs['location_id'] = lineitems_trxs['location_id'].astype(int)

                        # Grab all lineitems that don't have a promo code
                        lineitems_trxs_nopr = lineitems_trxs.loc[~lineitems_trxs['promo_code'].str.startswith("Pr")]
                        lineitems_trxs_nopr.fillna({'product_total_price': 0}, inplace=True)

                        # Get Unique Line Transaction IDs with Inside Dollars
                        line_trx_numbers = lineitems_trxs.groupby(['transaction_number']).agg(
                            inside_dollars = ('product_total_price', 'sum')
                        ).reset_index()

                        # Parent Level Lineitem Transactions
                        if not lineitems_trxs_nopr.empty:
                            # Parent Lineitem Transactions Grouping
                            parent_line_trxs = lineitems_trxs_nopr.groupby(['constant']).agg(
                                inside_transactions = ('transaction_number', pd.Series.nunique),
                                inside_sales = ('product_total_price', 'sum')
                            ).reset_index()
                            parent_line_trxs['constant'] = parent_line_trxs['constant'].astype(int)
                            parent_line_trxs['inside_sales'] = parent_line_trxs['inside_sales'].round(2)

                            # Site Lineitem Transactions Grouping
                            site_line_trxs = lineitems_trxs_nopr.groupby(['location_id']).agg(
                                inside_transactions = ('transaction_number', pd.Series.nunique),
                                inside_sales = ('product_total_price', 'sum')
                            ).reset_index()
                            site_line_trxs['location_id'] = site_line_trxs['location_id'].astype(int)

                    lineitems_end = datetime.datetime.now()
                    lineitems_total_seconds = str((lineitems_end - lineitems_start).total_seconds())

                    redemptions_start = datetime.datetime.now()
                    reds_query = """
                        SELECT
                            LocationIDRedeemed AS location_id,
                            CardNumber AS cardnumber,
                            trxid AS transaction_number,
                            Redeemed AS points_redeemed,
                            TrxDate AS redemption_date,
                            shiftnumber AS cashier_id
                        FROM redemption
                        WHERE
                            DATE(TrxDate) BETWEEN '{}' AND '{}' AND
                            LocationIDRedeemed IN ({})
                    """.format(start_date, end_date, location_ids)
                    reds = pd.read_sql(reds_query, con=loy_conn)

                    if reds.empty:
                        parent_reds = pd.DataFrame()
                        parent_reds['constant'] = 1
                        parent_reds['redeemed_pts'] = 0
                        parent_reds['unique_redemption_customers'] = 0
                        parent_reds['redemption_trxs'] = 0

                        site_reds = pd.DataFrame()
                        site_reds['location_id'] = loy_locations['location_id']
                        site_reds['redeemed_pts'] = 0
                        site_reds['unique_redemption_customers'] = 0
                        site_reds['redemption_trxs'] = 0

                    if not reds.empty:
                        reds.loc[:,'constant'] = 1
                        reds['location_id'] = reds['location_id'].astype(int)
                        reds.fillna({'transaction_number': 0}, inplace=True)

                        # Parent Redemptions Grouping
                        parent_reds = reds.groupby(['constant']).agg(
                            redeemed_pts = ('points_redeemed', 'sum'),
                            unique_redemption_customers = ('cardnumber', pd.Series.nunique),
                            redemption_trxs = ('transaction_number', pd.Series.nunique)
                        ).reset_index()
                        parent_reds['constant'] = parent_reds['constant'].astype(int)

                        # Site Redemptions Grouping
                        site_reds = reds.groupby(['location_id']).agg(
                            redeemed_pts = ('points_redeemed', 'sum'),
                            unique_redemption_customers = ('cardnumber', pd.Series.nunique),
                            redemption_trxs = ('transaction_number', pd.Series.nunique)
                        ).reset_index()
                        site_reds['location_id'] = site_reds['location_id'].astype(int)

                    redemptions_end = datetime.datetime.now()
                    redemptions_total_seconds = str((redemptions_end - redemptions_start).total_seconds())

                    members_start = datetime.datetime.now()
                    mem_query = """
                        SELECT
                            m.cardnumber AS cardnumber,
                            m.LastName AS last_name,
                            m.FirstName AS first_name,
                            m.MiddleName AS middle_name,
                            m.Address1 AS address_1,
                            m.Address2 AS address_2,
                            m.City AS city,
                            m.State AS state,
                            m.Zip AS zip,
                            m.Country AS country,
                            m.phone AS phone,
                            m.mobile AS mobile,
                            m.Email AS email,
                            DATE(m.ActivationDate) AS activation_date,
                            m.ActivationDate AS activation_datetime,
                            DATE(m.CancellationDate) AS cancellation_date,
                            m.CancellationDate AS cancellation_datetime,
                            DATE(m.LastPurchaseDate) AS last_purchase_date,
                            m.LastPurchaseDate AS last_purchase_datetime,
                            m.ActiveMember AS active_member,
                            m.BirthDate AS birth_date,
                            m.flag1 AS opt_in_status,
                            m.gender AS gender,
                            m.FirstTrxLocation AS first_trx_location,
                            m.FirstTrxDate AS first_trx_date,
                            m.FirstTrxCashier AS first_trx_cashier,
                            m.PersonalInfoModifiedDate AS personal_info_modified_date
                        FROM members m
                        INNER JOIN
                            (
                                SELECT
                                    a.cardnumber
                                FROM
                                    (
                                        SELECT
                                            m.cardnumber
                                        FROM
                                            members m
                                        INNER JOIN
                                            dailypointstrx d ON d.cardnumber = m.cardnumber
                                        WHERE
                                            d.TrxDate BETWEEN '{}' AND '{}' AND
                                            d.LocationID IN ({})
                                        UNION
                                        SELECT
                                            CardNumber
                                        FROM
                                            members
                                        WHERE
                                            FirstTrxLocation IN ({}) AND
                                            FirstTrxDate BETWEEN '{}' AND '{}'
                                    ) a
                            ) b ON b.cardnumber = m.cardnumber
                    """.format(startdate, enddate, location_ids, location_ids, start_date, end_date)
                    mem = pd.read_sql(mem_query, con=loy_conn)

                    if mem.empty:
                        parent_mem_enrolled_grp = pd.DataFrame()
                        parent_mem_enrolled_grp['constant'] = 1
                        parent_mem_enrolled_grp['enrolled_cards_used'] = 0
                        parent_mem_enrolled_grp['enrolled_trxs'] = 0

                        site_mem_enrolled_grp = pd.DataFrame()
                        site_mem_enrolled_grp['location_id'] = loy_locations['location_id']
                        site_mem_enrolled_grp['enrolled_cards_used'] = 0
                        site_mem_enrolled_grp['enrolled_trxs'] = 0

                        parent_mem_cards_issued = pd.DataFrame()
                        parent_mem_cards_issued['constant'] = 1
                        parent_mem_cards_issued['cards_issued'] = 0

                        site_mem_cards_issued = pd.DataFrame()
                        site_mem_cards_issued['location_id'] = loy_locations['location_id']
                        site_mem_cards_issued['cards_issued'] = 0

                        parent_mem_activation_date = pd.DataFrame()
                        parent_mem_activation_date['constant'] = 1
                        parent_mem_activation_date['card_enrollment'] = 0

                        site_mem_activation_date = pd.DataFrame()
                        site_mem_activation_date['location_id'] = loy_locations['location_id']
                        site_mem_activation_date['card_enrollment'] = 0

                    if not mem.empty:
                        mem['activation_datetime'] = mem['activation_datetime'].replace('0000-00-00 00:00:00', '')
                        mem['activation_datetime'] = mem.apply(convert_activation_to_datetime, axis=1)
                        mem['cancellation_datetime'] = mem['cancellation_datetime'].replace('0000-00-00 00:00:00', '')
                        mem['cancellation_datetime'] = mem.apply(convert_cancellation_to_datetime, axis=1)
                        mem['last_purchase_datetime'] = mem['last_purchase_datetime'].replace('0000-00-00 00:00:00', '')
                        mem['last_purchase_datetime'] = mem.apply(convert_lastpurchase_to_datetime, axis=1)
                        mem['personal_info_modified_date'] = mem['personal_info_modified_date'].replace('0000-00-00 00:00:00', '')
                        mem['personal_info_modified_date'] = mem.apply(convert_personalinfo_to_datetime, axis=1)
                        mem['birth_date'] = mem['birth_date'].replace('0000-00-00', '')
                        mem['birthdate'] = mem.apply(convert_birthdate_to_date, axis=1)
                        mem['first_trx_date'] = mem['first_trx_date'].replace('0000-00-00', '')
                        mem['first_trx_date'] = mem.apply(convert_firsttrxdate_to_date, axis=1)
                        mem.loc[:,'constant'] = 1

                        # Create members_enrolled_trxs dataframe
                        mem_enrolled = mem.loc[pd.notnull(mem['activation_datetime'])]
                        mem_enrolled_trxs = pd.merge(mem_enrolled, trxs_nobp, how='inner', on=['cardnumber'])

                        # See if this can be fixed
                        # A value is trying to be set on a copy of a slice from a DataFrame
                        mem_enrolled_trxs_filtered = mem_enrolled_trxs.loc[mem_enrolled_trxs['activation_datetime'] <= mem_enrolled_trxs['transaction_datetime']].copy()
                        mem_enrolled_trxs_filtered.rename(columns={'constant_x': 'constant'}, inplace=True)
                        mem_enrolled_trxs_filtered = mem_enrolled_trxs_filtered.drop(['constant_y'], axis=1)

                        if not mem_enrolled_trxs_filtered.empty:
                            # Parent Level Members Enrolled Grouping
                            parent_mem_enrolled_grp = mem_enrolled_trxs_filtered.groupby(['constant']).agg(
                                enrolled_cards_used = ('cardnumber', pd.Series.nunique),
                                enrolled_trxs = ('transaction_number', pd.Series.nunique)
                            ).reset_index()
                            parent_mem_enrolled_grp['constant'] = parent_mem_enrolled_grp['constant'].astype(int)

                            # Site Level Members Enrolled Grouping
                            site_mem_enrolled_grp = mem_enrolled_trxs_filtered.groupby(['location_id']).agg(
                                enrolled_cards_used = ('cardnumber', pd.Series.nunique),
                                enrolled_trxs = ('transaction_number', pd.Series.nunique)
                            ).reset_index()
                            site_mem_enrolled_grp['location_id'] = site_mem_enrolled_grp['location_id'].astype(int)
                        else:
                            parent_mem_enrolled_grp = pd.DataFrame()
                            parent_mem_enrolled_grp['constant'] = 1
                            parent_mem_enrolled_grp['enrolled_cards_used'] = 0
                            parent_mem_enrolled_grp['enrolled_trxs'] = 0

                            site_mem_enrolled_grp = pd.DataFrame()
                            site_mem_enrolled_grp['location_id'] = loy_locations['location_id']
                            site_mem_enrolled_grp['enrolled_cards_used'] = 0
                            site_mem_enrolled_grp['enrolled_trxs'] = 0

                        # Create members_first_transaction dataframe
                        mem_first_trxs = mem[(mem['first_trx_date'] >= start_date) & (mem['first_trx_date'] <= end_date)].copy()
                        mem_first_trxs.rename(columns={'first_trx_location': 'location_id'}, inplace=True)

                        if not mem_first_trxs.empty:
                            # Parent Level Member Cards Issued Grouping
                            parent_mem_cards_issued = mem_first_trxs.groupby(['constant']).agg(
                                cards_issued = ('cardnumber', pd.Series.nunique)
                            ).reset_index()
                            parent_mem_cards_issued['constant'] = parent_mem_cards_issued['constant'].astype(int)

                            # Site Level Member Cards Issued Grouping
                            site_mem_cards_issued = mem_first_trxs.groupby(['location_id']).agg(
                                cards_issued = ('cardnumber', pd.Series.nunique)
                            ).reset_index()
                            site_mem_cards_issued['location_id'] = site_mem_cards_issued['location_id'].astype(int)
                        else:
                            parent_mem_cards_issued = pd.DataFrame()
                            parent_mem_cards_issued['constant'] = 1
                            parent_mem_cards_issued['cards_issued'] = 0

                            site_mem_cards_issued = pd.DataFrame()
                            site_mem_cards_issued['location_id'] = loy_locations['location_id']
                            site_mem_cards_issued['cards_issued'] = 0

                        # Create members_activation_date dataframe
                        mem_act_date = mem[(mem['activation_datetime'] >= start_date) & (mem['activation_datetime'] <= end_date)].copy()
                        mem_act_date.rename(columns={'first_trx_location': 'location_id'}, inplace=True)
                        mem_act_date_final = mem_act_date[~mem_act_date.location_id.isin(exclude_locations_group)]

                        if not mem_act_date_final.empty:
                            # Parent Level Member Activation Date Grouping
                            parent_mem_activation_date = mem_act_date_final.groupby(['constant']).agg(
                                card_enrollment = ('cardnumber', pd.Series.nunique)
                            ).reset_index()
                            parent_mem_activation_date['constant'] = parent_mem_activation_date['constant'].astype(int)

                            # Site Level Member Activation Date Grouping
                            site_mem_activation_date = mem_act_date_final.groupby(['location_id']).agg(
                                card_enrollment = ('cardnumber', pd.Series.nunique)
                            ).reset_index()
                            site_mem_activation_date['location_id'] = site_mem_activation_date['location_id'].astype(int)
                        else:
                            parent_mem_activation_date = pd.DataFrame()
                            parent_mem_activation_date['constant'] = 1
                            parent_mem_activation_date['card_enrollment'] = 0

                            site_mem_activation_date = pd.DataFrame()
                            site_mem_activation_date['location_id'] = loy_locations['location_id']
                            site_mem_activation_date['card_enrollment'] = 0

                    members_end = datetime.datetime.now()
                    members_total_seconds = str((members_end - members_start).total_seconds())

                    # Fuel Only and Inside Only Process
                    if not trx_numbers.empty and not fuel_trx_numbers.empty and not line_trx_numbers.empty:
                        trx_nums_1 = pd.merge(trx_numbers, fuel_trx_numbers, how='left', on=['transaction_number'])
                        trx_nums_2 = pd.merge(trx_nums_1, line_trx_numbers, how='left', on=['transaction_number'])
                        trx_nums_2['constant'] = 1

                        # Fuel Only Transaction Numbers
                        fuel_only_trx_stage = trx_nums_2[trx_nums_2.total_fuel_gallons.notnull()]
                        fuel_only_trxs = fuel_only_trx_stage[~fuel_only_trx_stage.inside_dollars.notnull()]

                        if not fuel_only_trxs.empty:
                            # Parent Level Fuel Only Transaction Grouping
                            parent_fuel_only_trx_grp = fuel_only_trxs.groupby(['constant']).agg(
                                fuel_only_trx_count = ('transaction_number', pd.Series.nunique),
                                fuel_only_total_gallons = ('total_fuel_gallons', 'sum'),
                                fuel_only_customers = ('cardnumber', pd.Series.nunique)
                            ).reset_index()
                            parent_fuel_only_trx_grp['fuel_only_average_gallons'] = parent_fuel_only_trx_grp['fuel_only_total_gallons'] / parent_fuel_only_trx_grp['fuel_only_trx_count']

                            # Site Level Fuel Only Transaction Grouping
                            site_fuel_only_trx_grp = fuel_only_trxs.groupby(['location_id']).agg(
                                fuel_only_trx_count = ('transaction_number', pd.Series.nunique),
                                fuel_only_total_gallons = ('total_fuel_gallons', 'sum'),
                                fuel_only_customers = ('cardnumber', pd.Series.nunique)
                            ).reset_index()
                            site_fuel_only_trx_grp['fuel_only_average_gallons'] = site_fuel_only_trx_grp['fuel_only_total_gallons'] / site_fuel_only_trx_grp['fuel_only_trx_count']
                        else:
                            parent_fuel_only_trx_grp = pd.DataFrame()
                            parent_fuel_only_trx_grp['constant'] = 1
                            parent_fuel_only_trx_grp['fuel_only_trx_count'] = 0
                            parent_fuel_only_trx_grp['fuel_only_total_gallons'] = 0.000
                            parent_fuel_only_trx_grp['fuel_only_customers'] = 0
                            parent_fuel_only_trx_grp['fuel_only_average_gallons'] = 0.000

                            site_fuel_only_trx_grp = pd.DataFrame()
                            site_fuel_only_trx_grp['location_id'] = loy_locations['location_id']
                            site_fuel_only_trx_grp['fuel_only_trx_count'] = 0
                            site_fuel_only_trx_grp['fuel_only_total_gallons'] = 0.000
                            site_fuel_only_trx_grp['fuel_only_customers'] = 0
                            site_fuel_only_trx_grp['fuel_only_average_gallons'] = 0.000

                        # Inside Only Transaction Numbers
                        inside_only_trx_stage = trx_nums_2[trx_nums_2.inside_dollars.notnull()]
                        inside_only_trxs = inside_only_trx_stage[~inside_only_trx_stage.total_fuel_gallons.notnull()]

                        if not inside_only_trxs.empty:
                            # Parent Level Inside Only Transaction Grouping
                            parent_inside_only_trx_grp = inside_only_trxs.groupby(['constant']).agg(
                                inside_only_trx_count = ('transaction_number', pd.Series.nunique),
                                inside_only_total_dollars = ('inside_dollars', 'sum'),
                                inside_only_customers = ('cardnumber', pd.Series.nunique)
                            ).reset_index()
                            parent_inside_only_trx_grp['inside_only_ticket_avg'] = parent_inside_only_trx_grp['inside_only_total_dollars'] / parent_inside_only_trx_grp['inside_only_trx_count']

                            # Site Level Inside Only Transaction Grouping
                            site_inside_only_trx_grp = inside_only_trxs.groupby(['location_id']).agg(
                                inside_only_trx_count = ('transaction_number', pd.Series.nunique),
                                inside_only_total_dollars = ('inside_dollars', 'sum'),
                                inside_only_customers = ('cardnumber', pd.Series.nunique)
                            ).reset_index()
                            site_inside_only_trx_grp['inside_only_ticket_avg'] = site_inside_only_trx_grp['inside_only_total_dollars'] / site_inside_only_trx_grp['inside_only_trx_count']
                        else:
                            parent_inside_only_trx_grp = pd.DataFrame()
                            parent_inside_only_trx_grp['location_id'] = loy_locations['location_id']
                            parent_inside_only_trx_grp['inside_only_trx_count'] = 0
                            parent_inside_only_trx_grp['inside_only_total_dollars'] = 0.00
                            parent_inside_only_trx_grp['inside_only_customers'] = 0
                            parent_inside_only_trx_grp['inside_only_ticket_avg'] = 0.00

                            site_inside_only_trx_grp = pd.DataFrame()
                            site_inside_only_trx_grp['location_id'] = loy_locations['location_id']
                            site_inside_only_trx_grp['inside_only_trx_count'] = 0
                            site_inside_only_trx_grp['inside_only_total_dollars'] = 0.00
                            site_inside_only_trx_grp['inside_only_customers'] = 0
                            site_inside_only_trx_grp['inside_only_ticket_avg'] = 0.00

                    # Parent Level Business Review Joining All Parent Level DataFrames together
                    parent_stage = parent_trxs.merge(parent_trxs_bp, how='left', on=['constant'])\
                        .merge(parent_fuel_trxs, how='left', on=['constant'])\
                            .merge(parent_line_trxs, how='left', on=['constant'])\
                                .merge(parent_reds, how='left', on=['constant'])\
                                    .merge(parent_mem_enrolled_grp, how='left', on=['constant'])\
                                        .merge(parent_mem_cards_issued, how='left', on=['constant'])\
                                            .merge(parent_mem_activation_date, how='left', on=['constant'])\
                                                .merge(parent_fuel_only_trx_grp, how='left', on=['constant'])\
                                                    .merge(parent_inside_only_trx_grp, how='left', on=['constant'])
                    parent_stage['location_id'] = 0
                    parent_stage['location_name'] = 'Parent Corporate'

                    # Parent Level Business Review column ordering
                    parent_final = parent_stage[['location_id',
                                                 'location_name',
                                                 'total_transactions',
                                                 'unique_customers',
                                                 'total_nonbp_points_issued',
                                                 'total_dollars',
                                                 'total_gallons',
                                                 'total_bp_transactions',
                                                 'total_bp_points_issued',
                                                 'unique_fuel_customers',
                                                 'total_fuel_transactions',
                                                 'inside_transactions',
                                                 'inside_sales',
                                                 'redeemed_pts',
                                                 'unique_redemption_customers',
                                                 'redemption_trxs',
                                                 'enrolled_cards_used',
                                                 'enrolled_trxs',
                                                 'cards_issued',
                                                 'card_enrollment',
                                                 'fuel_only_trx_count',
                                                 'fuel_only_total_gallons',
                                                 'fuel_only_customers',
                                                 'fuel_only_average_gallons',
                                                 'inside_only_trx_count',
                                                 'inside_only_total_dollars',
                                                 'inside_only_customers',
                                                 'inside_only_ticket_avg']]

                    # Site Level Fillna
                    parent_final.fillna({'total_transactions': 0,
                                         'unique_customers': 0,
                                         'total_nonbp_points_issued': 0,
                                         'total_dollars': 0.00,
                                         'total_gallons': 0.000,
                                         'total_bp_transactions': 0,
                                         'total_bp_points_issued': 0,
                                         'unique_fuel_customers': 0,
                                         'total_fuel_transactions': 0,
                                         'inside_transactions': 0,
                                         'inside_sales': 0.00,
                                         'redeemed_pts': 0,
                                         'unique_redemption_customers': 0,
                                         'redemption_trxs': 0,
                                         'enrolled_cards_used': 0,
                                         'enrolled_trxs': 0,
                                         'cards_issued': 0,
                                         'card_enrollment': 0,
                                         'fuel_only_trx_count': 0,
                                         'fuel_only_total_gallons': 0.000,
                                         'fuel_only_customers': 0,
                                         'fuel_only_average_gallons': 0.000,
                                         'inside_only_trx_count': 0,
                                         'inside_only_total_dollars': 0.00,
                                         'inside_only_customers': 0,
                                         'inside_only_ticket_avg': 0}, inplace=True)

                    # Site Level Business Review
                    site_stage = loy_locations.merge(site_trxs, how='left', on=['location_id'])\
                        .merge(site_trxs_bp, how='left', on=['location_id'])\
                            .merge(site_fuel_trxs, how='left', on=['location_id'])\
                                .merge(site_line_trxs, how='left', on=['location_id'])\
                                    .merge(site_reds, how='left', on=['location_id'])\
                                        .merge(site_mem_enrolled_grp, how='left', on=['location_id'])\
                                            .merge(site_mem_cards_issued, how='left', on=['location_id'])\
                                                .merge(site_mem_activation_date, how='left', on=['location_id'])\
                                                    .merge(site_fuel_only_trx_grp, how='left', on=['location_id'])\
                                                        .merge(site_inside_only_trx_grp, how='left', on=['location_id'])

                    # Final Site Level DataFrame ordering
                    site_final = site_stage[['location_id',
                                             'location_name',
                                             'total_transactions',
                                             'unique_customers',
                                             'total_nonbp_points_issued',
                                             'total_dollars',
                                             'total_gallons',
                                             'total_bp_transactions',
                                             'total_bp_points_issued',
                                             'unique_fuel_customers',
                                             'total_fuel_transactions',
                                             'inside_transactions',
                                             'inside_sales',
                                             'redeemed_pts',
                                             'unique_redemption_customers',
                                             'redemption_trxs',
                                             'enrolled_cards_used',
                                             'enrolled_trxs',
                                             'cards_issued',
                                             'card_enrollment',
                                             'fuel_only_trx_count',
                                             'fuel_only_total_gallons',
                                             'fuel_only_customers',
                                             'fuel_only_average_gallons',
                                             'inside_only_trx_count',
                                             'inside_only_total_dollars',
                                             'inside_only_customers',
                                             'inside_only_ticket_avg']]

                    # Site Level Fillna
                    site_final.fillna({'total_transactions': 0,
                                       'unique_customers': 0,
                                       'total_nonbp_points_issued': 0,
                                       'total_dollars': 0.00,
                                       'total_gallons': 0.000,
                                       'total_bp_transactions': 0,
                                       'total_bp_points_issued': 0,
                                       'unique_fuel_customers': 0,
                                       'total_fuel_transactions': 0,
                                       'inside_transactions': 0,
                                       'inside_sales': 0.00,
                                       'redeemed_pts': 0,
                                       'unique_redemption_customers': 0,
                                       'redemption_trxs': 0,
                                       'enrolled_cards_used': 0,
                                       'enrolled_trxs': 0,
                                       'cards_issued': 0,
                                       'card_enrollment': 0,
                                       'fuel_only_trx_count': 0,
                                       'fuel_only_total_gallons': 0.000,
                                       'fuel_only_customers': 0,
                                       'fuel_only_average_gallons': 0.000,
                                       'inside_only_trx_count': 0,
                                       'inside_only_total_dollars': 0.00,
                                       'inside_only_customers': 0,
                                       'inside_only_ticket_avg': 0}, inplace=True)

                    # Combine Parent and Site Level
                    combined = [parent_final, site_final]
                    parent_site_combined = pd.concat(combined)

                    # Create Additional Calculated Columns
                    parent_site_combined['year_month'] = start_date[:7]
                    parent_site_combined['total_points_issued'] = (parent_site_combined['total_nonbp_points_issued'] + parent_site_combined['total_bp_points_issued'])
                    parent_site_combined['ticket_average'] = (parent_site_combined['total_dollars'] / parent_site_combined['total_transactions'])
                    parent_site_combined['frequency'] = (parent_site_combined['total_transactions'] / parent_site_combined['unique_customers'])
                    parent_site_combined['avg_points_redeemed'] = (parent_site_combined['redeemed_pts'] / parent_site_combined['redemption_trxs'])
                    parent_site_combined['member_avg_gallons'] = parent_site_combined['total_gallons'] / parent_site_combined['unique_fuel_customers']

                    # Fill empty column values with columns 0 datatype
                    parent_site_combined.fillna({'total_transactions': 0,
                                                 'unique_customers': 0,
                                                 'total_nonbp_points_issued': 0,
                                                 'total_dollars': 0.00,
                                                 'total_gallons': 0.000,
                                                 'total_bp_transactions': 0,
                                                 'total_bp_points_issued': 0,
                                                 'unique_fuel_customers': 0,
                                                 'total_fuel_transactions': 0,
                                                 'inside_transactions': 0,
                                                 'inside_sales': 0.00,
                                                 'redeemed_pts': 0,
                                                 'unique_redemption_customers': 0,
                                                 'redemption_trxs': 0,
                                                 'enrolled_cards_used': 0,
                                                 'enrolled_trxs': 0,
                                                 'cards_issued': 0,
                                                 'card_enrollment': 0,
                                                 'fuel_only_trx_count': 0,
                                                 'fuel_only_total_gallons': 0.000,
                                                 'fuel_only_customers': 0,
                                                 'fuel_only_average_gallons': 0.000,
                                                 'inside_only_trx_count': 0,
                                                 'inside_only_total_dollars': 0.00,
                                                 'inside_only_customers': 0,
                                                 'inside_only_ticket_avg': 0}, inplace=True)

                    # Modify column datatypes, typically from Float to Integer
                    parent_site_combined = parent_site_combined.astype({'total_transactions': 'int',
                                                                        'unique_customers': 'int',
                                                                        'total_nonbp_points_issued': 'int',
                                                                        'total_bp_transactions': 'int',
                                                                        'total_bp_points_issued': 'int',
                                                                        'unique_fuel_customers': 'int',
                                                                        'total_fuel_transactions': 'int',
                                                                        'inside_transactions': 'int',
                                                                        'redeemed_pts': 'int',
                                                                        'unique_redemption_customers': 'int',
                                                                        'redemption_trxs': 'int',
                                                                        'enrolled_cards_used': 'int',
                                                                        'enrolled_trxs': 'int',
                                                                        'cards_issued': 'int',
                                                                        'card_enrollment': 'int',
                                                                        'fuel_only_trx_count': 'int',
                                                                        'fuel_only_customers': 'int',
                                                                        'inside_only_trx_count': 'int',
                                                                        'inside_only_customers': 'int'})

                    # Final Business Review Column Ordering
                    br_final = parent_site_combined[['year_month',
                                                     'location_id',
                                                     'location_name',
                                                     'total_transactions',
                                                     'unique_customers',
                                                     'frequency',
                                                     'total_dollars',
                                                     'ticket_average',
                                                     'total_fuel_transactions',
                                                     'unique_fuel_customers',
                                                     'total_gallons',
                                                     'member_avg_gallons',
                                                     'total_nonbp_points_issued',
                                                     'total_bp_points_issued',
                                                     'total_points_issued',
                                                     'redeemed_pts',
                                                     'avg_points_redeemed',
                                                     'redemption_trxs',
                                                     'unique_redemption_customers',
                                                     'inside_only_trx_count',
                                                     'inside_only_total_dollars',
                                                     'inside_only_ticket_avg',
                                                     'inside_only_customers',
                                                     'fuel_only_trx_count',
                                                     'fuel_only_total_gallons',
                                                     'fuel_only_average_gallons',
                                                     'fuel_only_customers',
                                                     'cards_issued',
                                                     'card_enrollment',
                                                     'enrolled_cards_used',
                                                     'enrolled_trxs']]

                    # Export Business Review and set filename/filepath
                    br_filename = f"{customer_name}_business_review_{startdate}_to_{enddate}.csv"
                    br_filepath = f"{_WORKING_PATH}{br_filename}"
                    br_final.to_csv(br_filepath, sep=",", header=True, index=False)

                    print("Starting File Transfer to SFTP")
                    sftp_start = datetime.datetime.now()
                    try:
                        sftp_client = sftp_connect(sftp_host, sftp_user, sftp_password)
                        if sftp_put_file(sftp_client, _WORKING_PATH, br_filename, sftp_destination_path):
                            print(f"Epiphany Reporting file {br_filename} successfully submitted to {sftp_destination_path} for {customer_name}")
                        else:
                            print(f"Unable to upload {br_filename} because process is unable to push to the destination sftp site")
                    except Exception as exc:
                        print(f"Unable to submit {customer_name} Business Review to sftp site due to {exc}")
                    finally:
                        if sftp_client:
                            sftp_client.close()
                        sftp_client = None
                    sftp_end = datetime.datetime.now()
                    sftp_total_seconds = str((sftp_end - sftp_start).total_seconds())
                    print(f"Completed File Transfer to SFTP in {sftp_total_seconds} seconds")

            # Customer Time Metrics
            location_time_str = f"Location Information in {locations_total_seconds} Seconds"
            transaction_time_str = f"Transaction Information in {transactions_total_seconds} Seconds"
            lineitems_time_str = f"Lineitems Information in {lineitems_total_seconds} Seconds"
            redemptions_time_str = f"Redemption Inforamtion in {redemptions_total_seconds} Seconds"
            members_time_str = f"Member Information in {members_total_seconds} seconds"
            time_metrics_str = f"{location_time_str}\n{transaction_time_str}\n{lineitems_time_str}\n{redemptions_time_str}\n{members_time_str}"
            print(time_metrics_str)

            customer_end = datetime.datetime.now()
            customer_total_seconds = str((customer_end - customer_start).total_seconds())
            print(f"Completed {customer_name} Business Review in {customer_total_seconds} seconds.")

        # Skips to next customer if current customer name doesn't exist
        if not customer == customer_name:
            continue        

    business_review_end = datetime.datetime.now()
    business_review_total_seconds = str((business_review_end - business_review_start).total_seconds())
    print(f"***** Completed Business Review report process in {business_review_total_seconds} seconds. *****")

if __name__ == "__main__":
    main()