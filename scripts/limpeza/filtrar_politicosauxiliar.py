import multiprocessing as mp, pickle, re, os
from functools import partial

def achar_regex(regex, nome):
    if re.rearch(re.compile(regex[1]), nome[1]):
        return (regex[0], nome[0])
    else:
        return None

def main():
    arqvs = ['lista01.pickle', 'lista02.pickle']
    listas = []
    for arqv in arqvs:
        with open(f'dados/saida/{arqv}', 'rb') as fp:
            listas.append(pickle.load(fp))



if __name__ == '__main__':
    main()
    names = ['Brown', 'Wilson', 'Bartlett', 'Rivera', 'Molloy', 'Opie']
    with multiprocessing.Pool(processes=3) as pool:
        results = pool.starmap(merge_names, product(names, repeat=2))
    print(results)



import random
random.sample(lista01, 1)
random.sample(lista02, 1)

from functools import partial
import time
# achar as correspondências
start=time.time()
results = []
for i, nomes01 in enumerate(lista01[:500]):
    p = mp.Pool(processes=mp.cpu_count())
    function = partial(achar_regex, nomes01)
    p.map(function, lista02)
    p.close()
    p.join()
    if p:
        results.append(p)
end=time.time()
print(f'{(end-start)/60:.2f} time elapsed.')


resultados01 = results.copy()
len(resultados01)
resultados02 = results.copy()
