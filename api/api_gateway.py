from flask import Flask, request, jsonify
import os
from datetime import datetime
from scripts import indicadores_tasas
from scripts import indicadores_tasas_por_cliente
from scripts import indicadores_anexos
from scripts import indicadores_anexos_por_cliente
from scripts import indicadores_dias_credito
from scripts import indicadores_dias_credito_por_cliente
from scripts import grafica_total_facturado_x_condicion_pago
from scripts import grafica_total_facturado_x_condicion_pago_por_cliente
from scripts import parreto_clientes_comision
from scripts import grafica_clientes_x_condicion_pago
from scripts import grafica_clientes_x_condicion_pago_por_cliente

app = Flask(__name__)

@app.route('/api/update/indicadores_tasas_todos', methods=['GET'])
def indicadores_tasas_todos():
    if request.method == 'GET':
        print("Llamado a Función Indicadores Tasas")
        result,bool = indicadores_tasas.main()
        # Obtener la fecha hoy
        fecha_hoy = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if bool:
            return jsonify({'message': f'Successfull processed indicadores_tasas on {fecha_hoy}'})
        else:
            return jsonify({'message': f'Failed process indicadores_tasas on {fecha_hoy}, details: {result}'})


@app.route('/api/update/indicadores_tasas_por_cliente', methods=['GET'])
def indicadores_tasas_por_clientes():
    if request.method == 'GET':
        print("Llamado a Función Indicadores Tasas Por Cliente")
        result,bool = indicadores_tasas_por_cliente.main()
        # Obtener la fecha hoy
        fecha_hoy = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if bool:
            return jsonify({'message': f'Successfull process indicadores_tasas_por_cliente on {fecha_hoy}'})
        else:
            return jsonify({'message': f'Failed process indicadores_tasas_por_cliente on {fecha_hoy}, details: {result}'})
        

@app.route('/api/update/indicadores_anexos_todos', methods=['GET'])
def indicadores_anexos_todos():
    if request.method == 'GET':
        print("Llamado a Función Indicadores Anexos")
        result,bool = indicadores_anexos.main()
        # Obtener la fecha hoy
        fecha_hoy = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if bool:
            return jsonify({'message': f'Successfull processed indicadores_anexos on {fecha_hoy}'})
        else:
            return jsonify({'message': f'Failed process indicadores_anexos on {fecha_hoy}, details: {result}'})

@app.route('/api/update/indicadores_anexos_por_cliente', methods=['GET'])
def indicadores_anexos_por_clientes():
    if request.method == 'GET':
        print("Llamado a Función Indicadores Anexos Por Cliente")
        result,bool = indicadores_anexos_por_cliente.main()
        # Obtener la fecha hoy
        fecha_hoy = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if bool:
            return jsonify({'message': f'Successfull process indicadores_anexos_por_cliente on {fecha_hoy}'})
        else:
            return jsonify({'message': f'Failed process indicadores_anexos_por_cliente on {fecha_hoy}, details: {result}'})

@app.route('/api/update/indicadores_dias_credito_todos', methods=['GET'])
def indicadores_dias_credito_todos():
    if request.method == 'GET':
        print("Llamado a Función Indicadores Días Crédito")
        result,bool = indicadores_dias_credito.main()
        # Obtener la fecha hoy
        fecha_hoy = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if bool:
            return jsonify({'message': f'Successfull processed indicadores_dias_credito on {fecha_hoy}'})
        else:
            return jsonify({'message': f'Failed process indicadores_dias_credito on {fecha_hoy}, details: {result}'})


@app.route('/api/update/indicadores_dias_credito_por_cliente', methods=['GET'])
def indicadores_dias_credito_por_clientes():
    if request.method == 'GET':
        print("Llamado a Función Indicadores Días Crédito Por Cliente")
        result,bool = indicadores_dias_credito_por_cliente.main()
        # Obtener la fecha hoy
        fecha_hoy = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if bool:
            return jsonify({'message': f'Successfull process indicadores_dias_credito_por_cliente on {fecha_hoy}'})
        else:
            return jsonify({'message': f'Failed process indicadores_dias_credito_por_cliente on {fecha_hoy}, details: {result}'})
        
@app.route('/api/update/grafica_total_facturado_x_condicion_pago_todos', methods=['GET'])
def grafica_total_facturado_x_condicion_pago_todos():
    if request.method == 'GET':
        print("Llamado a Función Gráfica Total Facturado Por Condición de Pago")
        result,bool = grafica_total_facturado_x_condicion_pago.main()
        # Obtener la fecha hoy
        fecha_hoy = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if bool:
            return jsonify({'message': f'Successfull processed grafica_total_facturado_x_condicion_pago on {fecha_hoy}'})
        else:
            return jsonify({'message': f'Failed process grafica_total_facturado_x_condicion_pago on {fecha_hoy}, details: {result}'})


@app.route('/api/update/grafica_total_facturado_x_condicion_pago_por_cliente', methods=['GET'])
def grafica_total_facturado_x_condicion_pago_por_clientes():
    if request.method == 'GET':
        print("Llamado a Función Gráfica Total Facturado Por Condición de Pago Por Cliente")
        result,bool = grafica_total_facturado_x_condicion_pago_por_cliente.main()
        # Obtener la fecha hoy
        fecha_hoy = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if bool:
            return jsonify({'message': f'Successfull process grafica_total_facturado_x_condicion_pago_por_cliente on {fecha_hoy}'})
        else:
            return jsonify({'message': f'Failed process grafica_total_facturado_x_condicion_pago_por_cliente on {fecha_hoy}, details: {result}'})

@app.route('/api/update/parreto_clientes_comision', methods=['GET'])
def parreto_clientes_comisiones():
    if request.method == 'GET':
        print("Llamado a Función Indicadores Parreto Clientes Comisiones")
        result,bool = parreto_clientes_comision.main()
        # Obtener la fecha hoy
        fecha_hoy = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if bool:
            return jsonify({'message': f'Successfull process parreto_clientes_comision on {fecha_hoy}'})
        else:
            return jsonify({'message': f'Failed process parreto_clientes_comision on {fecha_hoy}, details: {result}'})

@app.route('/api/update/grafica_clientes_x_condicion_pago_todos', methods=['GET'])
def grafica_clientes_x_condicion_pago_todos():
    if request.method == 'GET':
        print("Llamado a Función Gráfica Clientes Por Condición de Pago")
        result,bool = grafica_clientes_x_condicion_pago.main()
        # Obtener la fecha hoy
        fecha_hoy = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if bool:
            return jsonify({'message': f'Successfull processed grafica_clientes_x_condicion_pago on {fecha_hoy}'})
        else:
            return jsonify({'message': f'Failed process grafica_clientes_x_condicion_pago on {fecha_hoy}, details: {result}'})


@app.route('/api/update/grafica_clientes_x_condicion_pago_por_cliente', methods=['GET'])
def grafica_clientes_x_condicion_pago_por_clientes():
    if request.method == 'GET':
        print("Llamado a Función Gráfica Clientes Por Condición de Pago Por Cliente")
        result,bool = grafica_clientes_x_condicion_pago_por_cliente.main()
        # Obtener la fecha hoy
        fecha_hoy = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if bool:
            return jsonify({'message': f'Successfull process grafica_clientes_x_condicion_pago_por_cliente on {fecha_hoy}'})
        else:
            return jsonify({'message': f'Failed process grafica_clientes_x_condicion_pago_por_cliente on {fecha_hoy}, details: {result}'})

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 8089))
    app.run(debug=True, host='0.0.0.0', port=port)