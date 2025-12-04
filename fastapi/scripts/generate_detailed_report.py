import pandas as pd
import glob
import os
import numpy as np
from datetime import datetime

def generate_detailed_report():
    # Configuração de caminhos
    input_path = r"c:\Users\Usuário\Documents\CESAR Trabalhos e Projetos\5 Periodo\Analise e Visualização de Dados\Projeto\Projeto-AVD\notebooks\dados_para_update_neon_*.csv"
    output_dir = r"c:\Users\Usuário\Documents\CESAR Trabalhos e Projetos\5 Periodo\Analise e Visualização de Dados\Projeto\Projeto-AVD\reports"
    output_file = os.path.join(output_dir, "relatorio_detalhado_para_graficos.md")
    
    os.makedirs(output_dir, exist_ok=True)
    
    files = glob.glob(input_path)
    all_data = []
    
    print(f"Processando {len(files)} arquivos...")

    # 1. Carregamento e Pré-processamento
    for file in files:
        city_name = os.path.basename(file).replace("dados_para_update_neon_", "").replace(".csv", "").replace("_", " ")
        try:
            df = pd.read_csv(file)
            if df.empty: continue
            
            df['data'] = pd.to_datetime(df['data'])
            df['cidade'] = city_name
            df['mes'] = df['data'].dt.month
            df['ano'] = df['data'].dt.year
            
            # Categorização do Vento (Escala Beaufort simplificada)
            conditions = [
                (df['velocidade_vento'] < 0.5),
                (df['velocidade_vento'] >= 0.5) & (df['velocidade_vento'] < 3.3),
                (df['velocidade_vento'] >= 3.3) & (df['velocidade_vento'] < 5.5),
                (df['velocidade_vento'] >= 5.5)
            ]
            choices = ['Calmo', 'Brisa Leve', 'Brisa Moderada', 'Vento Forte']
            df['categoria_vento'] = np.select(conditions, choices, default='Desconhecido')
            
            all_data.append(df)
        except Exception as e:
            print(f"Erro em {city_name}: {e}")

    if not all_data:
        print("Nenhum dado carregado.")
        return

    full_df = pd.concat(all_data, ignore_index=True)
    
    # Início da Escrita do Relatório
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("# Relatório Analítico Detalhado para Visualização de Dados\n\n")
        f.write(f"**Data de Geração:** {datetime.now().strftime('%d/%m/%Y %H:%M')}\n")
        f.write(f"**Total de Registros:** {len(full_df)}\n")
        f.write(f"**Cidades Analisadas:** {', '.join(full_df['cidade'].unique())}\n\n")
        
        # --- ANÁLISE 1: CICLO DIÁRIO (Para Gráficos de Linha/Área) ---
        f.write("## 1. Perfil Diário (Ciclo Circadiano)\n")
        f.write("*Ideal para: Gráficos de Linha (Eixo X: 0-23h, Eixo Y: Temp/Umid)*\n\n")
        f.write("Esta análise mostra como as variáveis se comportam ao longo das 24 horas do dia (média de todos os anos).\n\n")
        
        hourly = full_df.groupby(['cidade', 'hora'])[['temperatura', 'umidade', 'velocidade_vento']].mean().reset_index()
        
        for cidade in full_df['cidade'].unique():
            city_hourly = hourly[hourly['cidade'] == cidade]
            peak_temp_hour = city_hourly.loc[city_hourly['temperatura'].idxmax()]['hora']
            min_humid_hour = city_hourly.loc[city_hourly['umidade'].idxmin()]['hora']
            
            f.write(f"### {cidade}\n")
            f.write(f"- **Pico de Calor:** Ocorre às {int(peak_temp_hour)}h.\n")
            f.write(f"- **Momento Mais Seco:** Ocorre às {int(min_humid_hour)}h.\n")
            f.write("- **Dados para Plotagem (Resumo 6h em 6h):**\n")
            f.write("| Hora | Temperatura (°C) | Umidade (%) | Vento (m/s) |\n")
            f.write("|------|------------------|-------------|-------------|\n")
            for h in [0, 6, 12, 18]:
                row = city_hourly[city_hourly['hora'] == h].iloc[0]
                f.write(f"| {h:02d}:00 | {row['temperatura']:.1f} | {row['umidade']:.1f} | {row['velocidade_vento']:.1f} |\n")
            f.write("\n")

        # --- ANÁLISE 2: SAZONALIDADE (Para Heatmaps ou Bar Charts) ---
        f.write("## 2. Sazonalidade Mensal\n")
        f.write("*Ideal para: Heatmaps (X: Mês, Y: Cidade, Cor: Temp) ou Gráficos de Barra*\n\n")
        
        monthly = full_df.groupby(['cidade', 'mes'])['temperatura'].mean().reset_index()
        pivot_monthly = monthly.pivot(index='cidade', columns='mes', values='temperatura')
        
        f.write("### Média de Temperatura por Mês (°C)\n")
        f.write(pivot_monthly.to_string(float_format="%.1f"))
        f.write("\n\n")
        
        f.write("> **Insight:** Observe como as cidades do Sertão (ex: Floresta) mantêm médias altas quase o ano todo, enquanto o Agreste (Garanhuns) apresenta um 'inverno' visível no meio do ano.\n\n")

        # --- ANÁLISE 3: DISTRIBUIÇÃO E EXTREMOS (Para Boxplots) ---
        f.write("## 3. Distribuição e Estabilidade (Boxplot Data)\n")
        f.write("*Ideal para: Boxplots (Comparar estabilidade térmica entre cidades)*\n\n")
        
        stats = full_df.groupby('cidade')['temperatura'].describe()[['min', '25%', '50%', '75%', 'max']]
        f.write(stats.to_string(float_format="%.1f"))
        f.write("\n\n")
        
        # --- ANÁLISE 4: CORRELAÇÃO E DISPERSÃO (Para Scatter Plots) ---
        f.write("## 4. Relação Temperatura x Umidade\n")
        f.write("*Ideal para: Scatter Plots (Dispersão)*\n\n")
        
        f.write("Coeficiente de Correlação de Pearson (r):\n")
        correlations = full_df.groupby('cidade')[['temperatura', 'umidade']].corr().iloc[0::2, -1].reset_index()
        correlations = correlations.drop(columns=['level_1']).rename(columns={'umidade': 'Correlação'})
        f.write(correlations.to_string(float_format="%.2f"))
        f.write("\n\n")
        f.write("> **Interpretação:** Valores próximos de -1.0 indicam que quando a temperatura sobe, a umidade desce quase perfeitamente (física clássica). Valores mais fracos (ex: -0.4) indicam influência de outros fatores (como brisa marítima ou altitude).\n\n")

        # --- ANÁLISE 5: PERFIL DE VENTO (Para Gráficos de Pizza/Rosca) ---
        f.write("## 5. Perfil Eólico (Categorias)\n")
        f.write("*Ideal para: Gráficos de Pizza ou Rosca*\n\n")
        
        wind_counts = full_df.groupby(['cidade', 'categoria_vento']).size().unstack(fill_value=0)
        wind_pct = wind_counts.div(wind_counts.sum(axis=1), axis=0) * 100
        
        f.write("Porcentagem do tempo em cada categoria de vento:\n")
        f.write(wind_pct.to_string(float_format="%.1f"))
        f.write("\n\n")
        
        # --- ANÁLISE 6: RANKING DE RISCO (Para Gráficos de Radar) ---
        f.write("## 6. Indicadores de Risco (Para Gráfico de Radar)\n")
        f.write("*Ideal para: Gráfico de Radar comparando as cidades*\n\n")
        
        risk_df = full_df.groupby('cidade').agg(
            Horas_Secura_Extrema=('umidade', lambda x: (x < 20).sum()),
            Horas_Calor_Extremo=('temperatura', lambda x: (x > 35).sum()),
            Horas_Vento_Forte=('velocidade_vento', lambda x: (x > 5.5).sum())
        ).reset_index()
        
        f.write(risk_df.to_string())
        f.write("\n\n")

    print(f"Relatório gerado com sucesso em: {output_file}")
    print("Conteúdo pronto para criação de gráficos.")

if __name__ == "__main__":
    generate_detailed_report()