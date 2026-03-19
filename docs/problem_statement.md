# Problem statement

## Tytuł
Analiza popytu i przychodów kursów taxi w NYC na bazie danych wielkoskalowych

## Kontekst
Operatorzy transportowi i analitycy miejscy potrzebują wiedzieć:
- które obszary miasta generują największy ruch,
- kiedy występują piki popytu,
- jakie typy płatności i kursów dominują,
- gdzie pojawiają się anomalie w danych operacyjnych.

## Cel analityczny
Zbudować pipeline danych w architekturze medallion:
- **raw** – ładowanie danych źródłowych bez zmian,
- **silver** – oczyszczenie i standaryzacja,
- **gold** – agregaty do analizy biznesowej.

## Kluczowe pytania
1. Które strefy odbioru i miesiące generują największą liczbę kursów?
2. Które strefy generują najwyższy łączny przychód?
3. Jak rozkładają się typy płatności?

## Zakres danych
Dane NYC TLC dla wielu miesięcy i dwóch typów usług:
- yellow taxi,
- green taxi,
- słownik stref taxi.

## Miary końcowe
- liczba kursów,
- suma przychodu,
- średnia wartość kursu,
- średni dystans,
- liczba pasażerów,
- udział typów płatności.
