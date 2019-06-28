using System;

using NCalc;

namespace GatewayService
{
    class ExpressionCalculator
    {
        public string calculate_expression { get; set; }
        public Double a { get; set; }
        public Double b { get; set; }
        public Double c { get; set; }
        public Double d { get; set; }
        public Double e { get; set; }
        public Double f { get; set; }
        public Double g { get; set; }
        public Double h { get; set; }

        public object Clone()
        {
            return this.MemberwiseClone();
        }

        public ExpressionCalculator(string _calculate_expression, double _a, double _b, double _c, double _d, double _e, double _f, double _g, double _h)
        {
            this.calculate_expression = _calculate_expression;
            this.a = _a;
            this.b = _b;
            this.c = _c;
            this.d = _d;
            this.e = _e;
            this.f = _f;
            this.g = _g;
            this.h = _h;

        }

        public Double Evaluate()
        {
            Double douResult = 0;

            //Expression exp = new Expression("Sin(0) + 3 * 5 + Pi");
            Expression exp = new Expression(this.calculate_expression);

            //custom functions
            exp.EvaluateFunction += delegate (string name, FunctionArgs args)
            {
                switch (name)
                {
                    case "avg":
                        args.Result = (int)args.Parameters[0].Evaluate() + (int)args.Parameters[1].Evaluate();
                        break;
                    default:
                        //Console.WriteLine("Invalid selection. Please select 1, 2, or 3.");
                        break;
                }
            };

            //custom parameters
            exp.EvaluateParameter += delegate (string name, ParameterArgs args)
            {
                switch (name)
                {
                    case "Pi":
                        args.Result = 3.14;
                        break;
                    case "A":
                        args.Result = this.a;
                        break;
                    case "B":
                        args.Result = this.b;
                        break;
                    case "C":
                        args.Result = this.c;
                        break;
                    case "D":
                        args.Result = this.d;
                        break;
                    case "E":
                        args.Result = this.e;
                        break;
                    case "F":
                        args.Result = this.f;
                        break;
                    case "G":
                        args.Result = this.g;
                        break;
                    case "H":
                        args.Result = this.h;
                        break;

                    default:
                        //Console.WriteLine("Invalid selection. Please select 1, 2, or 3.");
                        break;
                }
            };

            try
            {
                Double.TryParse(exp.Evaluate().ToString(), out douResult);
            }
            catch (Exception ex)
            {
                douResult = -999999.999;
            }

            return douResult;

        }
    }
}
