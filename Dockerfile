# Use uma imagem base oficial do Python
FROM python:3.9-slim

# Defina o diretório de trabalho dentro do contêiner
WORKDIR /code

# Copie o arquivo de dependências para o diretório de trabalho
COPY requirements.txt .

# Instale as dependências
RUN pip install --no-cache-dir -r requirements.txt

# Copie todo o resto do seu código para o diretório de trabalho
COPY . .

# Diga ao Hugging Face em qual porta sua aplicação vai rodar
EXPOSE 7860

# O comando para iniciar sua aplicação (similar ao seu antigo Procfile)
CMD ["gunicorn", "app:server", "--bind", "0.0.0.0:7860"]