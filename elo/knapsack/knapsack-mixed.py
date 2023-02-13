from __future__ import print_function
from bs4 import BeautifulSoup
import requests
import json
import pandas as pd
# pip install ortools

from ortools.linear_solver import pywraplp

## Get -> prices/scores
base_url = 'https://www.fantasyxc.se/api/athletes'
with requests.Session() as s:
    r = s.get(base_url)
    soup = BeautifulSoup(r.content,'html5lib')

API_json = json.loads(soup.get_text())
API_df = pd.DataFrame.from_dict(pd.json_normalize(API_json), orient='columns')

## Filter ->
df = API_df.filter(items=["gender","is_team", "name", "athlete_id", "active","rank","country","score","price"])
df = df[(df.active == True) & (df.is_team == False) & (df.score > 0)]
df = df.reset_index(drop=True)

#df = pd.read_pickle("~/ski/elo/knapsack/fantasydf_spec.pkl")
df = pd.read_excel("~/ski/elo/knapsack/excel365/fantasydf_falun_relay.xlsx")
#print(df)#, drop=True)
df = df.reset_index(drop=True)


'''
NEED TO ADD:
- For each Race:
    1) Web scrape M/F start lists 
    2) Filter df by M/F start lists

'''

## Optimize ->

def knapsack(data):
  ## Dataframe Check:
  L = data.shape[0]
  if ((L-1)/2)*L != sum(data.index):
    print("ERROR: Remember to reset_index of dataframe ( df.reset_index(drop=True) )")
    return None

  ## Initialize Solver:
  solver = pywraplp.Solver('simple_mip_program',
                            pywraplp.Solver.CBC_MIXED_INTEGER_PROGRAMMING)
  ## Create Binary Varibles: 
  x = {}
  for j in range(data.shape[0]):
      x[j] = solver.IntVar(0, 1, 'x[%s]' % data["name"].iloc[j])
  
  ## Cost Constraint (< $100,000):
  constraint_expr = [data["price"].iloc[j] * x[j] for j in range(data.shape[0])]
  solver.Add(sum(constraint_expr) <= 100000.0)
  
  ## Amount Constraint (16 selections):
  constraint_expr = [x[j] for j in range(data.shape[0])]
  solver.Add(sum(constraint_expr) <= 16)

  men, women, mixed = [], [], []
  for j in range(data.shape[0]):
    if data["sex"].iloc[j] == "mixed":
      mixed.append(x[j])
    elif data["sex"].iloc[j] == "f":
      women.append(x[j])
    else:
      print("ERROR: Gender is either m or f.")
      return None
  
  ## Composition Constraint (8 men & 8 women):
  solver.Add(sum(mixed) <= 8)
  #solver.Add(sum(men) <= 8)
  #solver.Add(sum(women) <= 8)

  ## Objective:
  objective = solver.Objective()
  for j in range(L):
    objective.SetCoefficient(x[j], float(data['points'].iloc[j]))
  objective.SetMaximization()
  
  ## Solve:
  status = solver.Solve()

  if status == pywraplp.Solver.OPTIMAL:
    print('Total Score =', int(solver.Objective().Value()))
    Cost = []
    data["Solution"] = 0.0
    for j in range(L):
      if x[j].solution_value() > 0:
        Cost.append(data["price"].iloc[j])
        data.loc[j, 'Solution'] = 1
    print("Total Cost $",sum(Cost),'\n')
    print('Problem solved in %f milliseconds' % solver.wall_time())
    print('Problem solved in %d iterations' % solver.iterations())
    print('Problem solved in %d branch-and-bound nodes' % solver.nodes(),'\n')
    ## Print Picks:
    print(data[data.Solution > 0].sort_values(by=['sex', 'price'],ascending=False))
  else:
    print('The problem does not have an optimal solution.')
    return None
  return data

sol = knapsack(df)

## Visualize ->
'''
import seaborn as sns

sns.set_theme()

picks = sns.relplot(
    data=sol,
    x="price", y="score", col="gender",
    hue="Solution", style="Solution"
)
picks.fig.set_size_inches(20, 10)

'''