# 3 kluczowe problemy / ryzyka jakości danych

## 1. Niejednorodność schematu między plikami i latami
**Opis:**
Dane miesięczne mogą różnić się nazwami kolumn, typami lub obecnością części pól.

**Ryzyko:**
- błędy ładowania,
- błędne UNION-y,
- nieporównywalność rekordów między latami.

**Mitigacja:**
- jawne mapowanie kolumn do wspólnego schematu w silver,
- castowanie typów,
- wypełnianie brakujących pól `NULL`.

---

## 2. Nielogiczne wartości biznesowe
**Opis:**
W danych mogą występować rekordy z:
- ujemnym dystansem,
- zerową lub ujemną kwotą całkowitą,
- pickup po dropoff,
- skrajnie wysokim czasem przejazdu.

**Ryzyko:**
- zawyżone / zaniżone metryki,
- błędne agregacje przychodu,
- zła interpretacja popytu.

**Mitigacja:**
- filtrowanie rekordów nielogicznych w silver,
- dodatkowe flagi jakości,
- raport walidacyjny po ładowaniu.

---

## 3. Braki i niespójność kluczy referencyjnych
**Opis:**
Lokalizacje `PULocationID` i `DOLocationID` mogą nie mieć dopasowania do tabeli stref albo zawierać wartości puste / niepoprawne.

**Ryzyko:**
- utrata rekordów przy JOIN,
- błędna analiza przestrzenna,
- zafałszowanie rankingu stref.

**Mitigacja:**
- LEFT JOIN zamiast INNER JOIN w gold,
- oznaczanie brakujących stref jako `Unknown`,
- osobna metryka `% unmatched records`.
