import pandas as pd, numpy as np
p='/mnt/data/combined_5m.csv'
df=pd.read_csv(p,parse_dates=['timestamp']); df['day']=pd.to_datetime(df.day).dt.date; df=df.sort_values(['symbol','timestamp'])
def f(g):
 c=g.close;h=g.high;l=g.low;o=g.open;v=g.volume
 g=g.copy(); g['ema20']=c.ewm(span=20,adjust=False).mean();g['ema50']=c.ewm(span=50,adjust=False).mean();g['trend']=(g.ema20/g.ema50-1)*100
 g['atr']=pd.concat([h-l,(h-c.shift()).abs(),(l-c.shift()).abs()],axis=1).max(axis=1).rolling(14,min_periods=8).mean()/c*100
 g['vr']=v/v.rolling(20,min_periods=10).median();g['vwap']=((h+l+c)/3*v).groupby(g.day).cumsum()/v.groupby(g.day).cumsum();g['vd']=(c/g.vwap-1)*100
 g['bar']=g.groupby('day').cumcount();g['orh']=h.groupby(g.day).transform(lambda x:x.iloc[:6].max());g['orl']=l.groupby(g.day).transform(lambda x:x.iloc[:6].min())
 g['hh12']=h.shift().rolling(12,min_periods=12).max();g['ll12']=l.shift().rolling(12,min_periods=12).min();
 for k in [1,2,3,6]: g[f'f{k}']=c.shift(-k)/g.open.shift(-1)-1
 g['fh']=pd.concat([h.shift(-k) for k in range(1,7)],axis=1).max(axis=1)/g.open.shift(-1)-1
 g['fl']=pd.concat([l.shift(-k) for k in range(1,7)],axis=1).min(axis=1)/g.open.shift(-1)-1
 return g
df=df.groupby('symbol',group_keys=False).apply(f)
rules={
'OR_BREAK_L':(df.bar>=6)&(df.bar<=60)&(df.close>df.orh)&(df.vr>1.2)&(df.trend>0.05)&(df.vd>0),
'OR_BREAK_S':(df.bar>=6)&(df.bar<=60)&(df.close<df.orl)&(df.vr>1.2)&(df.trend<-0.05)&(df.vd<0),
'MOM3_L':(df.ret3 if 'ret3' in df else df.close.pct_change(3)>0.003)
}
# fix momentum
rules={
'OR_BREAK_L':(df.bar>=6)&(df.bar<=60)&(df.close>df.orh)&(df.vr>1.2)&(df.trend>0.05)&(df.vd>0),
'OR_BREAK_S':(df.bar>=6)&(df.bar<=60)&(df.close<df.orl)&(df.vr>1.2)&(df.trend<-0.05)&(df.vd<0),
'MOM3_L':(df.close.pct_change(3)>0.003)&(df.vr>1.3)&(df.trend>0.05)&(df.vd>0),
'MOM3_S':(df.close.pct_change(3)<-0.003)&(df.vr>1.3)&(df.trend<-0.05)&(df.vd<0),
'PULL_L':(df.ema20>df.ema50)&(df.close>df.ema20)&(df.close.shift(1)<df.ema20.shift(1))&(df.vr>1.0)&(df.vd>0),
'PULL_S':(df.ema20<df.ema50)&(df.close<df.ema20)&(df.close.shift(1)>df.ema20.shift(1))&(df.vr>1.0)&(df.vd<0),
'RETEST_L':(df.close>df.hh12)&(df.low<=df.hh12*1.001)&(df.close>df.open)&(df.vr>1.2)&(df.trend>0.05),
'RETEST_S':(df.close<df.ll12)&(df.high>=df.ll12*0.999)&(df.close<df.open)&(df.vr>1.2)&(df.trend<-0.05),
}
for n,m in rules.items():
 q=df.loc[m & (df.bar<69),['symbol','day','f6','fh','fl','atr']].dropna()
 if n.endswith('_S'): y=-q.f6; mfe=-q.fl
 else: y=q.f6;mfe=q.fh
 print(n,len(q),'mean%',y.mean()*100,'win',(y>0).mean()*100,'mfe%',mfe.mean()*100,'q75',mfe.quantile(.75)*100)
