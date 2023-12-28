# Teste Product Operations - Dynamox

    Teste para ingressar no time de Product Operations da Dynamox

# Sobre a Dynamox

A Dynamox é uma empresa de alta tecnologia que desenvolve sistemas de monitoramento e aquisição de dados de vibração e temperatura. 
Somos uma empresa especialista em análise de vibrações e monitoramento da condição de ativos industriais.

# Desafio

O objetivo deste desafio é testar suas habilidades com o python e manipulação de dados. 
Neste repositório, dentro da pasta databases, você vai encontrar alguns arquivos de extensão pickle e um arquivo json. 
Os arquivos pickle são partes de um database, que encontra-se segmentada, e o arquivo json contem o 
esquema requerido de saída dos dados.

OBS.: Não faça commit na master!!!

As bases de dados estão dividas em 3 partes (**sensors**, **gateway** e **measurements**):

A database **"sensors"** tem por finalidade registrar todos os sensores de uma determinada área. Esta database possui os campos de **'id'**, que se trata de uma identificação única, **'name'**, que pode ser entendido como um apelido que o cliente dá ao sensor. O campo **'gatewayId'** mostra a identificação de um dispositico ao qual o sensor encontra-se associado. Este dispositivo é chamado de gateway e ele faz coletas programadas do sensor, como base nas configurações indicadas. O campo **'start'** define a primeira data e horário da coleta para o sensor (datetime) e o campo **'frequency'** indica de quanto em quanto tempo o sensor será novamente coletado. Note que uma coleta só fica bem definida caso os dois campos estejam preenchidos com informações adequadas. Por fim o campo  **'signal'** indica o valor numérico da intensidade do sinal entre o sensor e o dispositivo de coleta. 

A database de **"gateway"** contem sua própria **'id'** (que na database anterior é chama de gatewayId), juntamente com seu nome (**name**).

Já a base de dados de **"measurements"** contem os campos de **'id'** da medida, **'sensor'** que indica a qual sensor essa medida pertence e  **'datetime'**, que informa a data e a hora em que a medida foi realizada. 

Com base no que foi descrito, pede-se que seja feito um código em python, cuja a saída seja um dataframe único que possua as colunas indicadas abaixo e que os tipos das colunas sejam os informados no **schema.json**. Os campos requeridos são:


1. **gateway_id**: liste todos as 'id' únicas dos gateways.

2. **gateway_name**: liste todos os nomes dos gateways em maiúsculo, separado do numeral através de espaço e numeral sempre com dois digitos (01, 02... etc). Exemplo: GATEWAY 01.

3. **number_of_registered_sensors**: número de sensores associado a cada um dos gateways. 

4. **valid_configurations**: se todos os sensores dentro de um determinado dispositivo possuírem os dois campos "início" e 
"frequência", preencha para cada gateway o campo indicado como 'True', se não 'False'. 

5. **percentual_valid_configurations**: preencha o campo com a porcentagem de sensores que possuem uma correta configuração (campos de 'start' e 'frequency' propriamente preenchidos) para cada dispositivo.

6. **expected_measurements**: preencha o campo com a quantidade de medidas que cada dispositivo gateway espera coletar com base na configuração do sensor, em 24 horas para cada dispositivo. DICA: utilize os campos de 'start' e 'frequency' para o cálculo de quantidade de ações em 24 horas para cada sensor.

7. **signal_mean_value**: calcule o valor médio do sinal para cada dispositivo. Trate os dados para retirar valores que não fazem sentido, como: "None", "NaN" ou valores positivos. 

8. **signal_status**: classifique o sinal de cada dispositivo, com base na seguinte regra: sinal < - 100 deve receber o status de "Ruim", -100 <= sinal < -90 
deve receber o status de "Regular" e acima de -90 de "Bom".

9. **signal_issue**: indique a quantidade de sensores associado ao dispositivo que possuem problemas no campo do sinal ("None", "NaN" ou valores positivos). 

10. **elapsed_time_since_last_measurement**: neste campo indique em dias e horas (relative deltatime) o tempo decorrido desde a coleta mais recente 
do dispositivo com base na data e hora atual. 

11. **measurement_status**: com base no deltatime do campo **elapsed_time_since_last_measurement**, classifique o status da ultima coleta, de acordo com a regra  valor < 60 dias, "coletado nos ultimos 60 dias", >= 60 como "coletado há mais de 60 dias" e caso não seja possível a comparação, classifique como "nunca coletado". 

12. **one_hour_groups**: Forneça o número de grupos com duas medidas coletadas em um intervalo menor ou igual que 1 hora para cada dispositivo, levando em consideração o "id" do sensor. 
DICA: Comece filtrando seu dataframe com base no "id" do sensor, depois organize o dataframe pelo datetime e por fim conte quantos pares aconteceram em menos de uma hora. Um par contado, deve ser descartado na sequência para não entrar novamente na próxima contagem.  

Ao finalizar envie o código para o repositório fazendo uma Pull Request. Para fazer o teste, você deve puxar uma branch da master,
utilizando o seguinte nome: "teste-seunome"

Também será avaliado:
    
    - Organização do código (utilize funções para organização dos processos)
    
    - Código limpo (não dê muitas voltas no que precisa ser feito, utilize variáveis que façam sentido para quem for ler seu código)
    
    - Testes unitários
        
 # Dúvidas sobre o teste?
    Envie um email para tamara.anjos@dynamox.net
